/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.schema;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Initializable;
import com.yahoo.bullet.typesystem.Type;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * BulletRecordField consists of a name, reference, and type. The name refers to the name of the field in the
 * BulletRecord. The reference points to the field of the object to extract. When the reference is not specified, the
 * field name is assumed to be the reference.
 * <br><br>
 * Note, for RECORD fields, the name and type should not be specified.
 */
public class BulletRecordField implements Initializable, Serializable {

    static final BulletError FIELD_REQUIRES_NAME = BulletError.makeError("The name of a field is missing.", "Please provide a name.");
    static final BulletError FIELD_INVALID_TYPE = BulletError.makeError("The type provided for a field is wrong.", "Please provide a valid supported type.");
    static final BulletError FIELD_NAME_DISALLOWS_DELIMITERS = BulletError.makeError("The name of a field contains a delimiter(s).", "Please provide a name without delimiters.");
    static final BulletError FIELD_REFERENCE_DISALLOWS_TRAILING_DELIMITERS = BulletError.makeError("The reference of a field contains a trailing delimiter(s).", "Please remove any trailing delimiters.");
    static final BulletError FIELD_RECORD_REQUIRES_NULL_NAME = BulletError.makeError("A field with record type cannot have a name.", "Please remove the name or set it to null.");
    static final BulletError FIELD_RECORD_REQUIRES_REFERENCE = BulletError.makeError("A field with record type must have a reference.", "Please provide a reference.");

    private static final String DELIMITER = ".";
    private static final String REGEX_DELIMITER = "\\.";

    @Getter
    @Setter(AccessLevel.PACKAGE)
    private String name;

    @Setter(AccessLevel.PACKAGE)
    private String reference;

    @Getter
    @Setter(AccessLevel.PACKAGE)
    private Type type;

    private transient String[] token;

    /**
     * Default constructor recommended by Gson.
     */
    public BulletRecordField() {
        name = null;
        reference = null;
        type = null;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        List<BulletError> errors = new ArrayList<>();
        if ((name == null || name.isEmpty()) && type != null) {
            errors.add(FIELD_REQUIRES_NAME);
        }
        if (name != null && name.contains(DELIMITER)) {
            errors.add(FIELD_NAME_DISALLOWS_DELIMITERS);
        }
        if (reference != null && (reference.startsWith(DELIMITER) || reference.endsWith(DELIMITER))) {
            errors.add(FIELD_REFERENCE_DISALLOWS_TRAILING_DELIMITERS);
        }
        if (type == null) {
            if (name != null) {
                errors.add(FIELD_RECORD_REQUIRES_NULL_NAME);
            }
            if (reference == null || reference.isEmpty()) {
                errors.add(FIELD_RECORD_REQUIRES_REFERENCE);
            }
        } else if (type == Type.NULL || !Type.ACTUAL_TYPES.contains(type)) {
            errors.add(FIELD_INVALID_TYPE);
        }
        if (reference == null) {
            reference = name;
        }
        return errors.isEmpty() ? Optional.empty() : Optional.of(errors);
    }

    /**
     * Gets the reference delimited as an array of tokens.
     *
     * @return The reference delimited as an array of tokens.
     */
    public String[] getToken() {
        if (token == null) {
            token = reference.split(REGEX_DELIMITER);
        }
        return token;
    }

    @Override
    public String toString() {
        return "{name: " + name + ", reference: " + reference + ", type: " + type + "}";
    }
}
