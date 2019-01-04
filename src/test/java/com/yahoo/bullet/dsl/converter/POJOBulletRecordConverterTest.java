/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.converter;

import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.record.BulletRecord;
import lombok.Getter;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;

public class POJOBulletRecordConverterTest {
    @Getter
    static class Foo {
        private Integer myInt = 123;
        private Long myLong = 456L;
        private Boolean myBool = true;
        private String myString = "789";
        private Double myDouble = 0.12;
        private Float myFloat = 3.45f;
        private Map<String, Integer> myIntMap = new HashMap<>();
        private List<Integer> myIntList = new ArrayList<>();
        private Integer myExcludedInt = 678;

        private Integer throwMyInt() throws Exception {
            throw new Exception();
        }
    }

    static class Bar {
        private HashMap<String, Boolean> myBoolMap = new HashMap<>();
        private Map<String, Integer> myIntMap = new HashMap<>();
        private Map<String, Long> myLongMap = new HashMap<>();
        private Map<String, Double> myDoubleMap = new HashMap<>();
        private Map<String, Float> myFloatMap = new HashMap<>();
        private Map<String, String> myStringMap = new HashMap<>();
        private Map<String, Map<String, Boolean>> myBoolMapMap = new HashMap<>();
        private Map<String, Map<String, Integer>> myIntMapMap = new HashMap<>();
        private Map<String, Map<String, Long>> myLongMapMap = new HashMap<>();
        private Map<String, Map<String, Double>> myDoubleMapMap = new HashMap<>();
        private Map<String, Map<String, Float>> myFloatMapMap = new HashMap<>();
        private Map<String, Map<String, String>> myStringMapMap = new HashMap<>();
        private List<Boolean> myBoolList = new ArrayList<>();
        private List<Integer> myIntList = new ArrayList<>();
        private List<Long> myLongList = new ArrayList<>();
        private List<Double> myDoubleList = new ArrayList<>();
        private List<Float> myFloatList = new ArrayList<>();
        private List<String> myStringList = new ArrayList<>();
        private List<Map<String, Boolean>> myBoolMapList = new ArrayList<>();
        private List<Map<String, Integer>> myIntMapList = new ArrayList<>();
        private List<Map<String, Long>> myLongMapList = new ArrayList<>();
        private List<Map<String, Double>> myDoubleMapList = new ArrayList<>();
        private List<Map<String, Float>> myFloatMapList = new ArrayList<>();
        private List<Map<String, String>> myStringMapList = new ArrayList<>();
    }

    @Test
    public void testConfigConstructor() throws Exception {
        BulletDSLConfig config = new BulletDSLConfig();
        config.set(BulletDSLConfig.RECORD_CONVERTER_POJO_CLASS_NAME, Foo.class.getName());

        Foo foo = new Foo();
        POJOBulletRecordConverter recordConverter = new POJOBulletRecordConverter(config);
        BulletRecord record = recordConverter.convert(foo);

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

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Could not find POJO class\\.")
    public void testConfigConstructorMissingClass() throws Exception {
        BulletDSLConfig config = new BulletDSLConfig();
        config.set(BulletDSLConfig.RECORD_CONVERTER_POJO_CLASS_NAME, "dne");

        new POJOBulletRecordConverter(config);
    }

    @Test
    public void testFromFoo() throws Exception {
        Foo foo = new Foo();
        POJOBulletRecordConverter recordConverter = new POJOBulletRecordConverter(Foo.class);

        BulletRecord record = recordConverter.convert(foo);
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

        // make some values null
        foo.myBool = null;
        foo.myString = null;
        foo.myFloat = null;

        record = recordConverter.convert(foo);
        Assert.assertEquals(record.get("myInt"), 123);
        Assert.assertEquals(record.get("myLong"), 456L);
        Assert.assertNull(record.get("myBool"));
        Assert.assertNull(record.get("myString"));
        Assert.assertEquals(record.get("myDouble"), 0.12);
        Assert.assertNull(record.get("myFloat"));
        Assert.assertEquals(record.get("myExcludedInt"), 678);
        Assert.assertNull(record.get("bar"));
        Assert.assertEquals(record.get("myIntMap"), foo.getMyIntMap());
        Assert.assertEquals(record.get("myIntList"), foo.getMyIntList());
    }

    @Test
    public void testFromFooSchema() throws Exception {
        Foo foo = new Foo();

        // This schema accepts lists all fields except myExcludedInt and also provides getters
        // Loads schema using class loader
        POJOBulletRecordConverter converter = new POJOBulletRecordConverter(Foo.class, "schemas/foo.json");

        BulletRecord record = converter.convert(foo);
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

        // make some values null
        foo.myBool = null;
        foo.myString = null;
        foo.myFloat = null;

        record = converter.convert(foo);
        Assert.assertNull(record.get("myExcludedInt"));
        Assert.assertEquals(record.get("myInt"), 123);
        Assert.assertEquals(record.get("myLong"), 456L);
        Assert.assertNull(record.get("myBool"));
        Assert.assertNull(record.get("myString"));
        Assert.assertEquals(record.get("myDouble"), 0.12);
        Assert.assertNull(record.get("myFloat"));
        Assert.assertNull(record.get("bar"));
        Assert.assertEquals(record.get("myIntMap"), foo.getMyIntMap());
        Assert.assertEquals(record.get("myIntList"), foo.getMyIntList());
    }

    @Test
    public void testFromBar() throws Exception {
        Bar bar = new Bar();

        POJOBulletRecordConverter converter = new POJOBulletRecordConverter(Bar.class);

        // The lists and maps in record are not copies
        BulletRecord record = converter.convert(bar);

        bar.myBoolMap.put("foo", true);
        bar.myBoolList.add(false);
        bar.myStringList.add("123");
        bar.myStringList.add("456");
        bar.myDoubleMap.put("hello", 0.12);
        bar.myIntMapMap.put("good", singletonMap("bye", 3));
        bar.myLongMapList.add(singletonMap("morning", 4L));

        Assert.assertEquals(record.get("myBoolMap"), bar.myBoolMap);
        Assert.assertEquals(record.get("myIntMap"), bar.myIntMap);
        Assert.assertEquals(record.get("myLongMap"), bar.myLongMap);
        Assert.assertEquals(record.get("myDoubleMap"), bar.myDoubleMap);
        Assert.assertEquals(record.get("myFloatMap"), bar.myFloatMap);
        Assert.assertEquals(record.get("myStringMap"), bar.myStringMap);
        Assert.assertEquals(record.get("myBoolMapMap"), bar.myBoolMapMap);
        Assert.assertEquals(record.get("myIntMapMap"), bar.myIntMapMap);
        Assert.assertEquals(record.get("myLongMapMap"), bar.myLongMapMap);
        Assert.assertEquals(record.get("myDoubleMapMap"), bar.myDoubleMapMap);
        Assert.assertEquals(record.get("myFloatMapMap"), bar.myFloatMapMap);
        Assert.assertEquals(record.get("myStringMapMap"), bar.myStringMapMap);
        Assert.assertEquals(record.get("myBoolList"), bar.myBoolList);
        Assert.assertEquals(record.get("myIntList"), bar.myIntList);
        Assert.assertEquals(record.get("myLongList"), bar.myLongList);
        Assert.assertEquals(record.get("myDoubleList"), bar.myDoubleList);
        Assert.assertEquals(record.get("myFloatList"), bar.myFloatList);
        Assert.assertEquals(record.get("myStringList"), bar.myStringList);
        Assert.assertEquals(record.get("myBoolMapList"), bar.myBoolMapList);
        Assert.assertEquals(record.get("myIntMapList"), bar.myIntMapList);
        Assert.assertEquals(record.get("myLongMapList"), bar.myLongMapList);
        Assert.assertEquals(record.get("myDoubleMapList"), bar.myDoubleMapList);
        Assert.assertEquals(record.get("myFloatMapList"), bar.myFloatMapList);
        Assert.assertEquals(record.get("myStringMapList"), bar.myStringMapList);
        Assert.assertNull(record.get("bar"));

        // make some values null
        bar.myLongList = null;
        bar.myFloatMapMap = null;

        record = converter.convert(bar);

        Assert.assertEquals(record.get("myBoolMap"), bar.myBoolMap);
        Assert.assertEquals(record.get("myIntMap"), bar.myIntMap);
        Assert.assertEquals(record.get("myLongMap"), bar.myLongMap);
        Assert.assertEquals(record.get("myDoubleMap"), bar.myDoubleMap);
        Assert.assertEquals(record.get("myFloatMap"), bar.myFloatMap);
        Assert.assertEquals(record.get("myStringMap"), bar.myStringMap);
        Assert.assertEquals(record.get("myBoolMapMap"), bar.myBoolMapMap);
        Assert.assertEquals(record.get("myIntMapMap"), bar.myIntMapMap);
        Assert.assertEquals(record.get("myLongMapMap"), bar.myLongMapMap);
        Assert.assertEquals(record.get("myDoubleMapMap"), bar.myDoubleMapMap);
        Assert.assertNull(record.get("myFloatMapMap"));
        Assert.assertEquals(record.get("myStringMapMap"), bar.myStringMapMap);
        Assert.assertEquals(record.get("myBoolList"), bar.myBoolList);
        Assert.assertEquals(record.get("myIntList"), bar.myIntList);
        Assert.assertNull(record.get("myLongList"));
        Assert.assertEquals(record.get("myDoubleList"), bar.myDoubleList);
        Assert.assertEquals(record.get("myFloatList"), bar.myFloatList);
        Assert.assertEquals(record.get("myStringList"), bar.myStringList);
        Assert.assertEquals(record.get("myBoolMapList"), bar.myBoolMapList);
        Assert.assertEquals(record.get("myIntMapList"), bar.myIntMapList);
        Assert.assertEquals(record.get("myLongMapList"), bar.myLongMapList);
        Assert.assertEquals(record.get("myDoubleMapList"), bar.myDoubleMapList);
        Assert.assertEquals(record.get("myFloatMapList"), bar.myFloatMapList);
        Assert.assertEquals(record.get("myStringMapList"), bar.myStringMapList);
        Assert.assertNull(record.get("bar"));
    }

    @Test
    public void testFromBarSchema() throws Exception {
        Bar bar = new Bar();

        // Loads schema using class loader
        POJOBulletRecordConverter converter = new POJOBulletRecordConverter(Bar.class, "schemas/bar.json");

        // The lists and maps in record are not copies
        BulletRecord record = converter.convert(bar);

        bar.myBoolMap.put("foo", true);
        bar.myBoolList.add(false);
        bar.myStringList.add("123");
        bar.myStringList.add("456");
        bar.myDoubleMap.put("hello", 0.12);
        bar.myIntMapMap.put("good", singletonMap("bye", 3));
        bar.myLongMapList.add(singletonMap("morning", 4L));

        Assert.assertEquals(record.get("myBoolMap"), bar.myBoolMap);
        Assert.assertNull(record.get("myIntMap"));
        Assert.assertNull(record.get("myLongMap"));
        Assert.assertEquals(record.get("myDoubleMap"), bar.myDoubleMap);
        Assert.assertNull(record.get("myFloatMap"));
        Assert.assertNull(record.get("myStringMap"));
        Assert.assertNull(record.get("myBoolMapMap"));
        Assert.assertEquals(record.get("myIntMapMap"), bar.myIntMapMap);
        Assert.assertNull(record.get("myLongMapMap"));
        Assert.assertNull(record.get("myDoubleMapMap"));
        Assert.assertNull(record.get("myFloatMapMap"));
        Assert.assertNull(record.get("myStringMapMap"));
        Assert.assertEquals(record.get("myBoolList"), bar.myBoolList);
        Assert.assertNull(record.get("myIntList"));
        Assert.assertNull(record.get("myLongList"));
        Assert.assertNull(record.get("myDoubleList"));
        Assert.assertNull(record.get("myFloatList"));
        Assert.assertEquals(record.get("myStringList"), bar.myStringList);
        Assert.assertNull(record.get("myBoolMapList"));
        Assert.assertNull(record.get("myIntMapList"));
        Assert.assertEquals(record.get("myLongMapList"), bar.myLongMapList);
        Assert.assertNull(record.get("myDoubleMapList"));
        Assert.assertNull(record.get("myFloatMapList"));
        Assert.assertNull(record.get("myStringMapList"));
        Assert.assertNull(record.get("bar"));

        // make some values null
        bar.myBoolMap = null;
        bar.myBoolList = null;

        record = converter.convert(bar);

        Assert.assertNull(record.get("myBoolMap"));
        Assert.assertNull(record.get("myIntMap"));
        Assert.assertNull(record.get("myLongMap"));
        Assert.assertEquals(record.get("myDoubleMap"), bar.myDoubleMap);
        Assert.assertNull(record.get("myFloatMap"));
        Assert.assertNull(record.get("myStringMap"));
        Assert.assertNull(record.get("myBoolMapMap"));
        Assert.assertEquals(record.get("myIntMapMap"), bar.myIntMapMap);
        Assert.assertNull(record.get("myLongMapMap"));
        Assert.assertNull(record.get("myDoubleMapMap"));
        Assert.assertNull(record.get("myFloatMapMap"));
        Assert.assertNull(record.get("myStringMapMap"));
        Assert.assertNull(record.get("myBoolList"));
        Assert.assertNull(record.get("myIntList"));
        Assert.assertNull(record.get("myLongList"));
        Assert.assertNull(record.get("myDoubleList"));
        Assert.assertNull(record.get("myFloatList"));
        Assert.assertEquals(record.get("myStringList"), bar.myStringList);
        Assert.assertNull(record.get("myBoolMapList"));
        Assert.assertNull(record.get("myIntMapList"));
        Assert.assertEquals(record.get("myLongMapList"), bar.myLongMapList);
        Assert.assertNull(record.get("myDoubleMapList"));
        Assert.assertNull(record.get("myFloatMapList"));
        Assert.assertNull(record.get("myStringMapList"));
        Assert.assertNull(record.get("bar"));
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Found member's type does not match field's type/subtype: \\{name: myInt, reference: throwMyInt, type: INTEGER, subtype: null\\}")
    public void testNotValidType() throws Exception {
        class Dummy {
            Byte throwMyInt;
        }
        new POJOBulletRecordConverter(Dummy.class, "src/test/resources/schemas/throw.json");
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Accessor for field not found: \\{name: myInt, reference: throwMyInt, type: INTEGER, subtype: null\\}")
    public void testNoSuchField() throws Exception {
        class Dummy {

        }
        new POJOBulletRecordConverter(Dummy.class, "src/test/resources/schemas/throw.json");
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Found method's return type does not match field's type/subtype: \\{name: myInt, reference: throwMyInt, type: INTEGER, subtype: null\\}")
    public void testNotValidGetter() throws Exception {
        class Dummy {
            Integer myInt;
            void throwMyInt() {
            }
        }
        new POJOBulletRecordConverter(Dummy.class, "src/test/resources/schemas/throw.json");
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Accessor for field not found: \\{name: myInt, reference: throwMyInt, type: INTEGER, subtype: null\\}")
    public void testNoSuchGetter() throws Exception {
        class Dummy {
            Integer myInt;
        }
        new POJOBulletRecordConverter(Dummy.class, "src/test/resources/schemas/throw.json");
    }

    @Test
    public void testUnsupportedTypes() throws Exception {
        class Dummy {
            Map<Integer, String> myMap;                     // key needs to be String
            Map<String, Byte> myByteMap;                    // value needs to be a supported primitive
            Map<String, List<String>> myListMap;            // can't have Map of List
            Map<String, Map<Integer, String>> myMapMap;     // inner key needs to be String
            Map<String, Map<String, Byte>> myByteMapMap;    // inner value needs to be a supported primitive
            List<List<String>> myListList;                  // can't have List of List
            List<Map<Integer, String>> myMapList;           // key needs to be String
            List<Map<String, Byte>> myByteMapList;          // value needs to be a supported primitive
            List<Map<String, List<Integer>>> myListMapList; // throws a class cast exception
        }
        POJOBulletRecordConverter recordConverter = new POJOBulletRecordConverter(Dummy.class);
        Assert.assertTrue(recordConverter.getAccessors().isEmpty());
    }

    @Test
    public void testEmptyClass() throws Exception {
        class Dummy {

        }
        // Ok
        BulletRecord record = new POJOBulletRecordConverter(Dummy.class).convert(new Dummy());
        Assert.assertEquals(record.fieldCount(), 0);
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Could not read or parse the schema file: .*")
    public void testMissingSchema() throws Exception {
        new POJOBulletRecordConverter(Foo.class, "does-not-exist.json");
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Could not read or parse the schema file: .*")
    public void testBadSchema() throws Exception {
        new POJOBulletRecordConverter(Foo.class, "src/test/resources/schemas/bad.json");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testConvertInaccessible() throws Exception {
        class Dummy {
            private String myString;
        }
        POJOBulletRecordConverter recordConverter = new POJOBulletRecordConverter(Dummy.class);

        Dummy dummy = new Dummy();
        dummy.myString = "hello";

        BulletRecord record = recordConverter.convert(dummy);

        Assert.assertEquals(record.get("myString"), "hello");

        // should never happen really
        recordConverter.getAccessors().get("myString").getValue().setAccessible(false);

        record = recordConverter.convert(dummy);

        Assert.assertNull(record.get("myString"));
    }

    @Test
    public void testConvertGetterThrows() throws Exception {
        POJOBulletRecordConverter recordConverter = new POJOBulletRecordConverter(Foo.class, "src/test/resources/schemas/throw.json");

        // accessor exists
        Assert.assertNotNull(recordConverter.getAccessors().get("throwMyInt").getKey());

        BulletRecord record = recordConverter.convert(new Foo());

        // nothing happens when exception is thrown
        Assert.assertNull(record.get("myInt"));
        Assert.assertEquals(record.fieldCount(), 0);
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Object is not of type: .*")
    public void testWrongType() throws Exception {
        class Dummy {

        }
        class SmartAlec {

        }
        new POJOBulletRecordConverter(Dummy.class).convert(new SmartAlec());
    }

    @Test
    public void testInvalidType() throws Exception {
        class Dummy {
            Byte myByte;
        }
        POJOBulletRecordConverter recordConverter = new POJOBulletRecordConverter(Dummy.class);
        Assert.assertTrue(recordConverter.getAccessors().isEmpty());
    }

    @Test
    public void testExtractValid() throws Exception {
        class Dummy {
            Map<String, Object> data;
        }
        class SmartAlec {
            Map<String, Object> data() {
                return null;
            }
        }
        POJOBulletRecordConverter recordConverter = new POJOBulletRecordConverter(Dummy.class, "schemas/record2.json");
        Assert.assertNotNull(recordConverter.getAccessors().get("data").getValue());

        recordConverter = new POJOBulletRecordConverter(SmartAlec.class, "schemas/record2.json");
        Assert.assertNotNull(recordConverter.getAccessors().get("data").getKey());
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Found base member's type is not map: \\{name: null, reference: data.aaa.bbb, type: RECORD, subtype: null\\}")
    public void testExtractInvalidField() throws Exception {
        class Dummy {
            Integer data;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/record2.json");
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Found base method's return type is not map: \\{name: null, reference: data.aaa.bbb, type: RECORD, subtype: null\\}")
    public void testExtractInvalidMethod() throws Exception {
        class Dummy {
            Integer data() {
                return null;
            }
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/record2.json");
    }

    @Test(expectedExceptions = BulletDSLException.class)
    public void testTypesMatchPrimitiveWrong() throws Exception {
        class Dummy {
            Integer bool;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/PRIMITIVE.json");
    }

    @Test
    public void testTypesMatchPrimitive() throws Exception {
        class Dummy {
            Boolean bool;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/PRIMITIVE.json");
    }

    @Test(expectedExceptions = BulletDSLException.class)
    public void testTypesMatchListWrongOuter() throws Exception {
        class Dummy {
            Integer list;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/LIST.json");
    }

    @Test(expectedExceptions = BulletDSLException.class)
    public void testTypesMatchListWrongInner() throws Exception {
        class Dummy {
            List<Integer> list;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/LIST.json");
    }

    @Test
    public void testTypesMatchList() throws Exception {
        class Dummy {
            List<Boolean> list;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/LIST.json");
    }

    @Test(expectedExceptions = BulletDSLException.class)
    public void testTypesMatchListOfMapWrongOuter() throws Exception {
        class Dummy {
            Integer listofmap;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/LISTOFMAP.json");
    }

    @Test(expectedExceptions = BulletDSLException.class)
    public void testTypesMatchListOfMapWrongInner() throws Exception {
        class Dummy {
            List<List<Integer>> listofmap;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/LISTOFMAP.json");
    }

    @Test(expectedExceptions = BulletDSLException.class)
    public void testTypesMatchListOfMapWrongInnerKey() throws Exception {
        class Dummy {
            List<Map<Integer, Integer>> listofmap;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/LISTOFMAP.json");
    }

    @Test(expectedExceptions = BulletDSLException.class)
    public void testTypesMatchListOfMapWrongInnerValue() throws Exception {
        class Dummy {
            List<Map<String, Integer>> listofmap;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/LISTOFMAP.json");
    }

    @Test
    public void testTypesMatchListOfMap() throws Exception {
        class Dummy {
            List<Map<String, Boolean>> listofmap;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/LISTOFMAP.json");
    }

    @Test(expectedExceptions = BulletDSLException.class)
    public void testTypesMatchMapWrongOuter() throws Exception {
        class Dummy {
            Integer map;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/MAP.json");
    }

    @Test(expectedExceptions = BulletDSLException.class)
    public void testTypesMatchMapWrongKey() throws Exception {
        class Dummy {
            Map<Integer, Integer> map;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/MAP.json");
    }

    @Test(expectedExceptions = BulletDSLException.class)
    public void testTypesMatchMapWrongValue() throws Exception {
        class Dummy {
            Map<String, Integer> map;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/MAP.json");
    }

    @Test
    public void testTypesMatchMap() throws Exception {
        class Dummy {
            Map<String, Boolean> map;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/MAP.json");
    }

    @Test(expectedExceptions = BulletDSLException.class)
    public void testTypesMatchMapOfMapWrongOuter() throws Exception {
        class Dummy {
            Integer mapofmap;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/MAPOFMAP.json");
    }

    @Test(expectedExceptions = BulletDSLException.class)
    public void testTypesMatchMapOfMapWrongOuterKey() throws Exception {
        class Dummy {
            Map<Integer, Integer> mapofmap;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/MAPOFMAP.json");
    }

    @Test(expectedExceptions = BulletDSLException.class)
    public void testTypesMatchMapOfMapWrongOuterValue() throws Exception {
        class Dummy {
            Map<String, List<Integer>> mapofmap;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/MAPOFMAP.json");
    }

    @Test(expectedExceptions = BulletDSLException.class)
    public void testTypesMatchMapOfMapWrongInnerKey() throws Exception {
        class Dummy {
            Map<String, Map<Integer, Integer>> mapofmap;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/MAPOFMAP.json");
    }

    @Test(expectedExceptions = BulletDSLException.class)
    public void testTypesMatchMapOfMapWrongInnerValue() throws Exception {
        class Dummy {
            Map<String, Map<String, Integer>> mapofmap;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/MAPOFMAP.json");
    }

    @Test
    public void testTypesMatchMapOfMap() throws Exception {
        class Dummy {
            Map<String, Map<String, Boolean>> mapofmap;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/MAPOFMAP.json");
    }

    @Test(expectedExceptions = BulletDSLException.class)
    public void testTypesMatchRecordWrongOuter() throws Exception {
        class Dummy {
            Integer record;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/RECORD.json");
    }

    @Test(expectedExceptions = BulletDSLException.class)
    public void testTypesMatchRecordWrongKey() throws Exception {
        class Dummy {
            Map<Integer, Object> record;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/RECORD.json");
    }

    @Test(expectedExceptions = BulletDSLException.class)
    public void testTypesMatchRecordBadKey() throws Exception {
        class Dummy {
            Map<Map<String, Boolean>, Object> record;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/RECORD.json");
    }

    @Test
    public void testTypesMatchRecord() throws Exception {
        class Dummy {
            Map<String, Object> record;
        }
        new POJOBulletRecordConverter(Dummy.class, "schemas/RECORD.json");
    }
}
