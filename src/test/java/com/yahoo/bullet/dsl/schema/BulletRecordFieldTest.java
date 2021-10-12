/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.schema;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.typesystem.Type;
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
    public void testInitializeMissingName() {
        field.setType(Type.BOOLEAN);

        // missing name
        Optional<List<BulletError>> optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_REQUIRES_NAME));
        Assert.assertEquals(optionalErrors.get().size(), 1);

        // empty name and reference
        field.setName("");
        optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_REQUIRES_NAME));
        Assert.assertEquals(optionalErrors.get().size(), 1);
    }

    @Test
    public void testInitializeNameWithDelimiters() {
        field.setName("aaa.bbb");
        field.setType(Type.BOOLEAN);

        Optional<List<BulletError>> optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_NAME_DISALLOWS_DELIMITERS));
        Assert.assertEquals(optionalErrors.get().size(), 1);
    }

    @Test
    public void testInitializeReferenceWithTrailingDelimiters() {
        field.setName("aaa");
        field.setReference(".aaa");
        field.setType(Type.BOOLEAN);

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
    public void testInitializeWithInvalidTypes() {
        field.setName("aaa");
        field.setType(Type.NULL);

        Optional<List<BulletError>> optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_INVALID_TYPE));
        Assert.assertEquals(optionalErrors.get().size(), 1);

        field.setType(Type.UNKNOWN);
        optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_INVALID_TYPE));
        Assert.assertEquals(optionalErrors.get().size(), 1);

        field.setType(Type.UNKNOWN_MAP);
        optionalErrors = field.initialize();
        Assert.assertFalse(optionalErrors.isPresent());

        field.setType(Type.UNKNOWN_LIST);
        optionalErrors = field.initialize();
        Assert.assertFalse(optionalErrors.isPresent());

        field.setType(Type.UNKNOWN_MAP_MAP);
        optionalErrors = field.initialize();
        Assert.assertFalse(optionalErrors.isPresent());

        field.setType(Type.UNKNOWN_MAP_LIST);
        optionalErrors = field.initialize();
        Assert.assertFalse(optionalErrors.isPresent());
    }

    @Test
    public void testInitializeWithValidTypes() {
        field.setName("aaa");
        field.setType(Type.BOOLEAN);
        Assert.assertFalse(field.initialize().isPresent());
        field.setType(Type.STRING_LIST);
        Assert.assertFalse(field.initialize().isPresent());
        field.setType(Type.FLOAT_MAP_LIST);
        Assert.assertFalse(field.initialize().isPresent());
        field.setType(Type.DOUBLE_MAP);
        Assert.assertFalse(field.initialize().isPresent());
        field.setType(Type.BOOLEAN_MAP_MAP);
        Assert.assertFalse(field.initialize().isPresent());
    }

    @Test
    public void testInitializeRecord() {
        // missing reference
        field.setName("aaa");

        Optional<List<BulletError>> optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_RECORD_REQUIRES_NULL_NAME));
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_RECORD_REQUIRES_REFERENCE));
        Assert.assertEquals(optionalErrors.get().size(), 2);

        // empty reference
        field.setReference("");

        optionalErrors = field.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_RECORD_REQUIRES_NULL_NAME));
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordField.FIELD_RECORD_REQUIRES_REFERENCE));
        Assert.assertEquals(optionalErrors.get().size(), 2);

        // no errors
        field.setName(null);
        field.setReference("aaa");

        Assert.assertFalse(field.initialize().isPresent());
    }

    @Test
    public void testInitializeToken() {
        field.setName("aaa");
        field.setType(Type.STRING);
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
        field.setType(Type.STRING_MAP_MAP);
        Assert.assertEquals(field.toString(), "{name: aaa, reference: bbb, type: STRING_MAP_MAP}");
    }
}
