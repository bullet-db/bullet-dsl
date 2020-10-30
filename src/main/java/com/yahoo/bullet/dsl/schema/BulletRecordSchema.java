/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.schema;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Initializable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A BulletRecordSchema consists of a list of {@link BulletRecordField}. This class is used by BulletRecordConverter to
 * decide which fields from an object to extract and put in a BulletRecord.
 */
@Getter
@Setter(AccessLevel.PACKAGE)
public class BulletRecordSchema implements Initializable, Serializable {

    public static final BulletError SCHEMA_REQUIRES_FIELDS = BulletError.makeError("The fields list is null or empty.", "Please provide a non-empty list of fields.");
    public static final BulletError SCHEMA_REQUIRES_UNIQUE_FIELD_NAMES = BulletError.makeError("Field names must be non-null and unique.", "Please use unique field names.");

    private List<BulletRecordField> fields;

    /**
     * Default constructor recommended by Gson.
     */
    public BulletRecordSchema() {
        fields = null;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (fields == null || fields.isEmpty()) {
            return Optional.of(Collections.singletonList(SCHEMA_REQUIRES_FIELDS));
        }
        List<BulletRecordField> nonRecordFields = fields.stream().filter(recordField -> recordField.getType() != null).collect(Collectors.toList());
        Set<String> names = nonRecordFields.stream().map(BulletRecordField::getName).collect(Collectors.toSet());
        if (names.size() < nonRecordFields.size()) {
            return Optional.of(Collections.singletonList(SCHEMA_REQUIRES_UNIQUE_FIELD_NAMES));
        }
        List<BulletError> errors = new ArrayList<>();
        fields.forEach(f -> f.initialize().ifPresent(errors::addAll));
        return errors.isEmpty() ? Optional.empty() : Optional.of(errors);
    }
}
