/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.schema;

import com.yahoo.bullet.common.BulletError;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

public class BulletRecordFieldTest {

    private BulletRecordField field;

    @BeforeMethod
    public void setup() {
        field = new BulletRecordField();
    }

    @Test
    public void testInitializeMissingNameAndType() {
        // missing name
        Optional<List<BulletError>> optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_REQUIRES_NAME));
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_REQUIRES_TYPE));
        Assert.assertEquals(optionalErrors.get().size(), 2);

        // empty name
        field.setName("");
        optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_REQUIRES_NAME));
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_REQUIRES_TYPE));
        Assert.assertEquals(optionalErrors.get().size(), 2);
    }

    @Test
    public void testInitializeNameWithDelimiters() {
        field.setName("aaa.bbb");
        field.setType(BulletRecordField.Type.BOOLEAN);

        Optional<List<BulletError>> optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_NAME_DISALLOWS_DELIMITERS));
        Assert.assertEquals(optionalErrors.get().size(), 1);
    }

    @Test
    public void testInitializeReferenceWithTrailingDelimiters() {
        field.setName("aaa");
        field.setReference(".aaa");
        field.setType(BulletRecordField.Type.BOOLEAN);

        Optional<List<BulletError>> optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_REFERENCE_DISALLOWS_TRAILING_DELIMITERS));
        Assert.assertEquals(optionalErrors.get().size(), 1);

        field.setReference("aaa.");
        optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_REFERENCE_DISALLOWS_TRAILING_DELIMITERS));
        Assert.assertEquals(optionalErrors.get().size(), 1);

        field.setReference(".aaa.");
        optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_REFERENCE_DISALLOWS_TRAILING_DELIMITERS));
        Assert.assertEquals(optionalErrors.get().size(), 1);

        // non-trailing delimiters are ok
        field.setReference("aaa.bbb");

        Assert.assertFalse(field.initialize().isPresent());
    }

    @Test
    public void testInitializePrimitive() {
        field.setName("aaa");
        field.setType(BulletRecordField.Type.BOOLEAN);

        Assert.assertFalse(field.initialize().isPresent());
    }

    @Test
    public void testInitializePrimitiveWithSubtype() {
        field.setName("aaa");
        field.setType(BulletRecordField.Type.STRING);
        field.setSubtype(BulletRecordField.Type.BOOLEAN);

        Optional<List<BulletError>> optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_PRIMITIVE_REQUIRES_NULL_SUBTYPE));
        Assert.assertEquals(optionalErrors.get().size(), 1);
    }

    @Test
    public void testInitializeList() {
        field.setName("aaa");
        field.setType(BulletRecordField.Type.LIST);

        Optional<List<BulletError>> optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_LIST_OR_MAP_REQUIRES_PRIMITIVE_SUBTYPE));
        Assert.assertEquals(optionalErrors.get().size(), 1);

        field.setSubtype(BulletRecordField.Type.LIST);

        optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_LIST_OR_MAP_REQUIRES_PRIMITIVE_SUBTYPE));
        Assert.assertEquals(optionalErrors.get().size(), 1);

        field.setSubtype(BulletRecordField.Type.INTEGER);

        Assert.assertFalse(field.initialize().isPresent());
    }

    @Test
    public void testInitializeListOfMap() {
        field.setName("aaa");
        field.setType(BulletRecordField.Type.LISTOFMAP);

        Optional<List<BulletError>> optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_LIST_OR_MAP_REQUIRES_PRIMITIVE_SUBTYPE));
        Assert.assertEquals(optionalErrors.get().size(), 1);

        field.setSubtype(BulletRecordField.Type.LISTOFMAP);

        optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_LIST_OR_MAP_REQUIRES_PRIMITIVE_SUBTYPE));
        Assert.assertEquals(optionalErrors.get().size(), 1);

        field.setSubtype(BulletRecordField.Type.LONG);

        Assert.assertFalse(field.initialize().isPresent());
    }

    @Test
    public void testInitializeMap() {
        field.setName("aaa");
        field.setType(BulletRecordField.Type.MAP);

        Optional<List<BulletError>> optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_LIST_OR_MAP_REQUIRES_PRIMITIVE_SUBTYPE));
        Assert.assertEquals(optionalErrors.get().size(), 1);

        field.setSubtype(BulletRecordField.Type.MAP);

        optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_LIST_OR_MAP_REQUIRES_PRIMITIVE_SUBTYPE));
        Assert.assertEquals(optionalErrors.get().size(), 1);

        field.setSubtype(BulletRecordField.Type.FLOAT);

        Assert.assertFalse(field.initialize().isPresent());
    }

    @Test
    public void testInitializeMapOfMap() {
        field.setName("aaa");
        field.setType(BulletRecordField.Type.MAPOFMAP);

        Optional<List<BulletError>> optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_LIST_OR_MAP_REQUIRES_PRIMITIVE_SUBTYPE));
        Assert.assertEquals(optionalErrors.get().size(), 1);

        field.setSubtype(BulletRecordField.Type.MAPOFMAP);

        optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_LIST_OR_MAP_REQUIRES_PRIMITIVE_SUBTYPE));
        Assert.assertEquals(optionalErrors.get().size(), 1);

        field.setSubtype(BulletRecordField.Type.DOUBLE);

        Assert.assertFalse(field.initialize().isPresent());
    }

    @Test
    public void testInitializeRecord() {
        // missing reference
        field.setName("aaa");
        field.setType(BulletRecordField.Type.RECORD);
        field.setSubtype(BulletRecordField.Type.BOOLEAN);

        Optional<List<BulletError>> optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_RECORD_REQUIRES_NULL_NAME));
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_RECORD_REQUIRES_REFERENCE));
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_RECORD_REQUIRES_NULL_SUBTYPE));
        Assert.assertEquals(optionalErrors.get().size(), 3);

        // empty reference
        field.setReference("");

        optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_RECORD_REQUIRES_NULL_NAME));
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_RECORD_REQUIRES_REFERENCE));
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_RECORD_REQUIRES_NULL_SUBTYPE));
        Assert.assertEquals(optionalErrors.get().size(), 3);

        // no errors
        field.setName(null);
        field.setReference("aaa");
        field.setSubtype(null);

        Assert.assertFalse(field.initialize().isPresent());
    }

    @Test
    public void testInitializeToken() {
        field.setName("aaa");
        field.setType(BulletRecordField.Type.STRING);
        field.setReference("aaa.bbb.ccc.ddd.eee");

        Assert.assertFalse(field.initialize().isPresent());

        Assert.assertEquals(field.getToken().length, 5);
        Assert.assertEquals(field.getToken()[0], "aaa");
        Assert.assertEquals(field.getToken()[1], "bbb");
        Assert.assertEquals(field.getToken()[2], "ccc");
        Assert.assertEquals(field.getToken()[3], "ddd");
        Assert.assertEquals(field.getToken()[4], "eee");
    }

    @Test
    public void testToString() {
        field.setName("aaa");
        field.setReference("bbb");
        field.setType(BulletRecordField.Type.MAPOFMAP);
        field.setSubtype(BulletRecordField.Type.LONG);

        Assert.assertEquals(field.toString(), "{name: aaa, reference: bbb, type: MAPOFMAP, subtype: LONG}");
    }

    @Test
    public void testTypeValueOf() {
        // coverage
        Assert.assertEquals(BulletRecordField.Type.valueOf("MAPOFMAP"), BulletRecordField.Type.MAPOFMAP);
    }
}
