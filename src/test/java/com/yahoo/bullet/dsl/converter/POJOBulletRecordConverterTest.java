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

        Assert.assertEquals(record.typedGet("myInt").getValue(), 123);
        Assert.assertEquals(record.typedGet("myLong").getValue(), 456L);
        Assert.assertEquals(record.typedGet("myBool").getValue(), true);
        Assert.assertEquals(record.typedGet("myString").getValue(), "789");
        Assert.assertEquals(record.typedGet("myDouble").getValue(), 0.12);
        Assert.assertEquals(record.typedGet("myFloat").getValue(), 3.45f);
        Assert.assertEquals(record.typedGet("myExcludedInt").getValue(), 678);
        Assert.assertFalse(record.hasField("bar"));
        Assert.assertEquals(record.typedGet("myIntMap").getValue(), foo.getMyIntMap());
        Assert.assertEquals(record.typedGet("myIntList").getValue(), foo.getMyIntList());
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
        Assert.assertEquals(record.typedGet("myInt").getValue(), 123);
        Assert.assertEquals(record.typedGet("myLong").getValue(), 456L);
        Assert.assertEquals(record.typedGet("myBool").getValue(), true);
        Assert.assertEquals(record.typedGet("myString").getValue(), "789");
        Assert.assertEquals(record.typedGet("myDouble").getValue(), 0.12);
        Assert.assertEquals(record.typedGet("myFloat").getValue(), 3.45f);
        Assert.assertEquals(record.typedGet("myExcludedInt").getValue(), 678);
        Assert.assertFalse(record.hasField("bar"));
        Assert.assertEquals(record.typedGet("myIntMap").getValue(), foo.getMyIntMap());
        Assert.assertEquals(record.typedGet("myIntList").getValue(), foo.getMyIntList());

        // make some values null
        foo.myBool = null;
        foo.myString = null;
        foo.myFloat = null;

        record = recordConverter.convert(foo);
        Assert.assertEquals(record.typedGet("myInt").getValue(), 123);
        Assert.assertEquals(record.typedGet("myLong").getValue(), 456L);
        Assert.assertFalse(record.hasField("myBool"));
        Assert.assertFalse(record.hasField("myString"));
        Assert.assertEquals(record.typedGet("myDouble").getValue(), 0.12);
        Assert.assertFalse(record.hasField("myFloat"));
        Assert.assertEquals(record.typedGet("myExcludedInt").getValue(), 678);
        Assert.assertFalse(record.hasField("bar"));
        Assert.assertEquals(record.typedGet("myIntMap").getValue(), foo.getMyIntMap());
        Assert.assertEquals(record.typedGet("myIntList").getValue(), foo.getMyIntList());
    }

    @Test
    public void testFromFooSchema() throws Exception {
        Foo foo = new Foo();

        // This schema accepts lists all fields except myExcludedInt and also provides getters
        // Loads schema using class loader
        POJOBulletRecordConverter converter = new POJOBulletRecordConverter(Foo.class, "schemas/foo.json");

        BulletRecord record = converter.convert(foo);
        Assert.assertFalse(record.hasField("myExcludedInt"));
        Assert.assertEquals(record.typedGet("myInt").getValue(), 123);
        Assert.assertEquals(record.typedGet("myLong").getValue(), 456L);
        Assert.assertEquals(record.typedGet("myBool").getValue(), true);
        Assert.assertEquals(record.typedGet("myString").getValue(), "789");
        Assert.assertEquals(record.typedGet("myDouble").getValue(), 0.12);
        Assert.assertEquals(record.typedGet("myFloat").getValue(), 3.45f);
        Assert.assertFalse(record.hasField("bar"));
        Assert.assertEquals(record.typedGet("myIntMap").getValue(), foo.getMyIntMap());
        Assert.assertEquals(record.typedGet("myIntList").getValue(), foo.getMyIntList());

        // make some values null
        foo.myBool = null;
        foo.myString = null;
        foo.myFloat = null;

        record = converter.convert(foo);
        Assert.assertFalse(record.hasField("myExcludedInt"));
        Assert.assertEquals(record.typedGet("myInt").getValue(), 123);
        Assert.assertEquals(record.typedGet("myLong").getValue(), 456L);
        Assert.assertFalse(record.hasField("myBool"));
        Assert.assertFalse(record.hasField("myString"));
        Assert.assertEquals(record.typedGet("myDouble").getValue(), 0.12);
        Assert.assertFalse(record.hasField("myFloat"));
        Assert.assertFalse(record.hasField("bar"));
        Assert.assertEquals(record.typedGet("myIntMap").getValue(), foo.getMyIntMap());
        Assert.assertEquals(record.typedGet("myIntList").getValue(), foo.getMyIntList());
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

        Assert.assertEquals(record.typedGet("myBoolMap").getValue(), bar.myBoolMap);
        Assert.assertEquals(record.typedGet("myIntMap").getValue(), bar.myIntMap);
        Assert.assertEquals(record.typedGet("myLongMap").getValue(), bar.myLongMap);
        Assert.assertEquals(record.typedGet("myDoubleMap").getValue(), bar.myDoubleMap);
        Assert.assertEquals(record.typedGet("myFloatMap").getValue(), bar.myFloatMap);
        Assert.assertEquals(record.typedGet("myStringMap").getValue(), bar.myStringMap);
        Assert.assertEquals(record.typedGet("myBoolMapMap").getValue(), bar.myBoolMapMap);
        Assert.assertEquals(record.typedGet("myIntMapMap").getValue(), bar.myIntMapMap);
        Assert.assertEquals(record.typedGet("myLongMapMap").getValue(), bar.myLongMapMap);
        Assert.assertEquals(record.typedGet("myDoubleMapMap").getValue(), bar.myDoubleMapMap);
        Assert.assertEquals(record.typedGet("myFloatMapMap").getValue(), bar.myFloatMapMap);
        Assert.assertEquals(record.typedGet("myStringMapMap").getValue(), bar.myStringMapMap);
        Assert.assertEquals(record.typedGet("myBoolList").getValue(), bar.myBoolList);
        Assert.assertEquals(record.typedGet("myIntList").getValue(), bar.myIntList);
        Assert.assertEquals(record.typedGet("myLongList").getValue(), bar.myLongList);
        Assert.assertEquals(record.typedGet("myDoubleList").getValue(), bar.myDoubleList);
        Assert.assertEquals(record.typedGet("myFloatList").getValue(), bar.myFloatList);
        Assert.assertEquals(record.typedGet("myStringList").getValue(), bar.myStringList);
        Assert.assertEquals(record.typedGet("myBoolMapList").getValue(), bar.myBoolMapList);
        Assert.assertEquals(record.typedGet("myIntMapList").getValue(), bar.myIntMapList);
        Assert.assertEquals(record.typedGet("myLongMapList").getValue(), bar.myLongMapList);
        Assert.assertEquals(record.typedGet("myDoubleMapList").getValue(), bar.myDoubleMapList);
        Assert.assertEquals(record.typedGet("myFloatMapList").getValue(), bar.myFloatMapList);
        Assert.assertEquals(record.typedGet("myStringMapList").getValue(), bar.myStringMapList);
        Assert.assertFalse(record.hasField("bar"));

        // make some values null
        bar.myLongList = null;
        bar.myFloatMapMap = null;

        record = converter.convert(bar);

        Assert.assertEquals(record.typedGet("myBoolMap").getValue(), bar.myBoolMap);
        Assert.assertEquals(record.typedGet("myIntMap").getValue(), bar.myIntMap);
        Assert.assertEquals(record.typedGet("myLongMap").getValue(), bar.myLongMap);
        Assert.assertEquals(record.typedGet("myDoubleMap").getValue(), bar.myDoubleMap);
        Assert.assertEquals(record.typedGet("myFloatMap").getValue(), bar.myFloatMap);
        Assert.assertEquals(record.typedGet("myStringMap").getValue(), bar.myStringMap);
        Assert.assertEquals(record.typedGet("myBoolMapMap").getValue(), bar.myBoolMapMap);
        Assert.assertEquals(record.typedGet("myIntMapMap").getValue(), bar.myIntMapMap);
        Assert.assertEquals(record.typedGet("myLongMapMap").getValue(), bar.myLongMapMap);
        Assert.assertEquals(record.typedGet("myDoubleMapMap").getValue(), bar.myDoubleMapMap);
        Assert.assertEquals(record.typedGet("myStringMapMap").getValue(), bar.myStringMapMap);
        Assert.assertEquals(record.typedGet("myBoolList").getValue(), bar.myBoolList);
        Assert.assertEquals(record.typedGet("myIntList").getValue(), bar.myIntList);
        Assert.assertEquals(record.typedGet("myDoubleList").getValue(), bar.myDoubleList);
        Assert.assertEquals(record.typedGet("myFloatList").getValue(), bar.myFloatList);
        Assert.assertEquals(record.typedGet("myStringList").getValue(), bar.myStringList);
        Assert.assertEquals(record.typedGet("myBoolMapList").getValue(), bar.myBoolMapList);
        Assert.assertEquals(record.typedGet("myIntMapList").getValue(), bar.myIntMapList);
        Assert.assertEquals(record.typedGet("myLongMapList").getValue(), bar.myLongMapList);
        Assert.assertEquals(record.typedGet("myDoubleMapList").getValue(), bar.myDoubleMapList);
        Assert.assertEquals(record.typedGet("myFloatMapList").getValue(), bar.myFloatMapList);
        Assert.assertEquals(record.typedGet("myStringMapList").getValue(), bar.myStringMapList);
        Assert.assertFalse(record.hasField("myLongList"));
        Assert.assertFalse(record.hasField("myFloatMapMap"));
        Assert.assertFalse(record.hasField("bar"));
    }

    @Test
    public void testFromBarSchema() throws Exception {
        Bar bar = new Bar();

        // Loads schema using class loader
        POJOBulletRecordConverter converter = new POJOBulletRecordConverter(Bar.class, "schemas/bar.json");

        // The lists and maps in record are not copies!!
        BulletRecord record = converter.convert(bar);

        bar.myBoolMap.put("foo", true);
        bar.myBoolList.add(false);
        bar.myStringList.add("123");
        bar.myStringList.add("456");
        bar.myDoubleMap.put("hello", 0.12);
        bar.myIntMapMap.put("good", singletonMap("bye", 3));
        bar.myLongMapList.add(singletonMap("morning", 4L));

        Assert.assertEquals(record.typedGet("myBoolMap").getValue(), bar.myBoolMap);
        Assert.assertFalse(record.hasField("myIntMap"));
        Assert.assertFalse(record.hasField("myLongMap"));
        Assert.assertEquals(record.typedGet("myDoubleMap").getValue(), bar.myDoubleMap);
        Assert.assertFalse(record.hasField("myFloatMap"));
        Assert.assertFalse(record.hasField("myStringMap"));
        Assert.assertFalse(record.hasField("myBoolMapMap"));
        Assert.assertEquals(record.typedGet("myIntMapMap").getValue(), bar.myIntMapMap);
        Assert.assertFalse(record.hasField("myLongMapMap"));
        Assert.assertFalse(record.hasField("myDoubleMapMap"));
        Assert.assertFalse(record.hasField("myFloatMapMap"));
        Assert.assertFalse(record.hasField("myStringMapMap"));
        Assert.assertEquals(record.typedGet("myBoolList").getValue(), bar.myBoolList);
        Assert.assertFalse(record.hasField("myIntList"));
        Assert.assertFalse(record.hasField("myLongList"));
        Assert.assertFalse(record.hasField("myDoubleList"));
        Assert.assertFalse(record.hasField("myFloatList"));
        Assert.assertEquals(record.typedGet("myStringList").getValue(), bar.myStringList);
        Assert.assertFalse(record.hasField("myBoolMapList"));
        Assert.assertFalse(record.hasField("myIntMapList"));
        Assert.assertEquals(record.typedGet("myLongMapList").getValue(), bar.myLongMapList);
        Assert.assertFalse(record.hasField("myDoubleMapList"));
        Assert.assertFalse(record.hasField("myFloatMapList"));
        Assert.assertFalse(record.hasField("myStringMapList"));
        Assert.assertFalse(record.hasField("bar"));

        // make some values null
        bar.myBoolMap = null;
        bar.myBoolList = null;

        record = converter.convert(bar);

        Assert.assertFalse(record.hasField("myBoolMap"));
        Assert.assertFalse(record.hasField("myIntMap"));
        Assert.assertFalse(record.hasField("myLongMap"));
        Assert.assertEquals(record.typedGet("myDoubleMap").getValue(), bar.myDoubleMap);
        Assert.assertFalse(record.hasField("myFloatMap"));
        Assert.assertFalse(record.hasField("myStringMap"));
        Assert.assertFalse(record.hasField("myBoolMapMap"));
        Assert.assertEquals(record.typedGet("myIntMapMap").getValue(), bar.myIntMapMap);
        Assert.assertFalse(record.hasField("myLongMapMap"));
        Assert.assertFalse(record.hasField("myDoubleMapMap"));
        Assert.assertFalse(record.hasField("myFloatMapMap"));
        Assert.assertFalse(record.hasField("myStringMapMap"));
        Assert.assertFalse(record.hasField("myBoolList"));
        Assert.assertFalse(record.hasField("myIntList"));
        Assert.assertFalse(record.hasField("myLongList"));
        Assert.assertFalse(record.hasField("myDoubleList"));
        Assert.assertFalse(record.hasField("myFloatList"));
        Assert.assertEquals(record.typedGet("myStringList").getValue(), bar.myStringList);
        Assert.assertFalse(record.hasField("myBoolMapList"));
        Assert.assertFalse(record.hasField("myIntMapList"));
        Assert.assertEquals(record.typedGet("myLongMapList").getValue(), bar.myLongMapList);
        Assert.assertFalse(record.hasField("myDoubleMapList"));
        Assert.assertFalse(record.hasField("myFloatMapList"));
        Assert.assertFalse(record.hasField("myStringMapList"));
        Assert.assertFalse(record.hasField("bar"));
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

        Assert.assertEquals(record.typedGet("myString").getValue(), "hello");

        // should never happen really
        recordConverter.getAccessors().get("myString").getValue().setAccessible(false);

        record = recordConverter.convert(dummy);

        Assert.assertFalse(record.hasField("myString"));
    }

    @Test
    public void testConvertGetterThrows() throws Exception {
        POJOBulletRecordConverter recordConverter = new POJOBulletRecordConverter(Foo.class, "src/test/resources/schemas/throw.json");

        // accessor exists
        Assert.assertNotNull(recordConverter.getAccessors().get("throwMyInt").getKey());

        BulletRecord record = recordConverter.convert(new Foo());

        // nothing happens when exception is thrown
        Assert.assertFalse(record.hasField("myInt"));
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
