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


import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class BulletRecordSchemaTest {

    private BulletRecordSchema schema;

    @BeforeMethod
    public void setup() {
        schema = new BulletRecordSchema();
    }

    @Test
    public void testInitializeNullFields() {
        Optional<List<BulletError>> optionalErrors = schema.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordSchema.SCHEMA_REQUIRES_FIELDS));
    }

    @Test
    public void testInitializeEmptyFields() {
        schema.setFields(Collections.emptyList());

        Optional<List<BulletError>> optionalErrors = schema.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordSchema.SCHEMA_REQUIRES_FIELDS));
    }

    @Test
    public void testInitializeNonUniqueFieldNames() {
        BulletRecordField field = new BulletRecordField();
        field.setName("aaa");
        field.setType(Type.STRING);

        schema.setFields(Arrays.asList(field, field));

        Optional<List<BulletError>> optionalErrors = schema.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        Assert.assertTrue(optionalErrors.get().contains(BulletRecordSchema.SCHEMA_REQUIRES_UNIQUE_FIELD_NAMES));
    }

    @Test
    public void testInitializeWithField() {
        BulletRecordField field = new BulletRecordField();
        field.setName("aaa");

        schema.setFields(Collections.singletonList(field));
        Assert.assertTrue(schema.initialize().isPresent());

        field.setType(Type.BOOLEAN);
        Assert.assertFalse(schema.initialize().isPresent());
    }
}
