/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.converter;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.dsl.schema.BulletRecordField;
import com.yahoo.bullet.dsl.schema.BulletRecordSchema;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * BulletRecordConverter is used to convert objects into BulletRecords. Converters should extend this class and expect
 * configuration though {@link BulletDSLConfig}. If a {@link BulletRecordSchema} is provided, and type-checking is
 * enabled, the converter will check that values match their types in the schema and throw if they do not. If a schema is
 * not provided, the level of type-checking is left to the implementation.
 */
public abstract class BulletRecordConverter implements Serializable {

    private static final long serialVersionUID = -8337322656873297988L;
    private BulletRecordProvider provider;
    protected BulletDSLConfig config;
    protected BulletRecordSchema schema;
    protected boolean shouldTypeCheck = false;

    /**
     * Constructor that takes a configuration containing the settings relevant for this converter.
     *
     * @param bulletConfig The {@link BulletConfig} to use.
     */
    public BulletRecordConverter(BulletConfig bulletConfig) {
        this.config = new BulletDSLConfig(bulletConfig);
    }

    /**
     * Helper function used to initialize BulletRecordConverter from BulletDSLConfig. The default implementation sets the
     * {@link BulletRecordProvider}, and optionally the {@link BulletRecordSchema}, used by the converter.
     *
     * @return This initialized BulletRecordConverter.
     * @throws BulletDSLException if there is an error creating the converter.
     */
    protected BulletRecordConverter build() throws BulletDSLException {
        String recordProviderClassName = config.getAs(BulletDSLConfig.RECORD_PROVIDER_CLASS_NAME, String.class);
        String schemaFile = config.getAs(BulletDSLConfig.RECORD_CONVERTER_SCHEMA_FILE, String.class);

        shouldTypeCheck = config.getAs(BulletDSLConfig.RECORD_CONVERTER_SCHEMA_TYPE_CHECK_ENABLE, Boolean.class);
        provider = BulletRecordProvider.from(recordProviderClassName);

        if (schemaFile != null) {
            try (JsonReader reader = getJsonReader(schemaFile)) {
                schema = new Gson().fromJson(reader, BulletRecordSchema.class);
            } catch (Exception e) {
                throw new BulletDSLException("Could not read or parse the schema file: " + schemaFile, e);
            }
            Optional<List<BulletError>> errors = schema.initialize();
            if (errors.isPresent()) {
                throw new BulletDSLException(errors.get().toString());
            }
        }
        return this;
    }

    /**
     * Converts an object to a BulletRecord using the configuration-defined implementation of BulletRecord.
     *
     * @param object The object to be converted.
     * @return A BulletRecord with fields from object.
     * @throws BulletDSLException if there is an error converting the object to a BulletRecord.
     */
    public BulletRecord convert(Object object) throws BulletDSLException {
        return convert(object, provider.getInstance());
    }

    /**
     * Converts an object to a BulletRecord using the given record. The default implementation assumes a valid schema.
     *
     * @param object The object to be converted.
     * @param record The record to insert fields into.
     * @return The record with additional fields from object.
     * @throws BulletDSLException if there is an error converting the object to a BulletRecord.
     */
    @SuppressWarnings("unchecked")
    public BulletRecord convert(Object object, BulletRecord record) throws BulletDSLException {
        for (BulletRecordField field : schema.getFields()) {
            try {
                Object value = extract(object, field.getToken());
                if (value == null) {
                    continue;
                }
                setField(field, value, record);
            } catch (Exception e) {
                throw new BulletDSLException("Could not convert field: " + field, e);
            }
        }
        return record;
    }

    /**
     * Sets the field in a {@link BulletRecord}.
     *
     * @param field The field to set.
     * @param value The value to set the field to.
     * @param record The record to set the field in.
     */
    @SuppressWarnings("unchecked")
    protected void setField(BulletRecordField field, Object value, BulletRecord record) {
        Type type = field.getType();
        // Record
        if (type == null) {
            flattenMap((Map<String, Serializable>) value, record);
            return;
        }
        String name = field.getName();
        TypedObject object = getTypedObject(name, type, (Serializable) value);
        record.typedSet(name, object);
    }

    /**
     * Takes the fields of a map and inserts them into a BulletRecord.
     *
     * @param mapRecord The map to take fields from.
     * @param record The BulletRecord to insert fields into.
     */
    protected void flattenMap(Map<String, Serializable> mapRecord, BulletRecord record) {
        mapRecord.forEach(
            (k, v) -> {
                if (v != null) {
                    record.typedSet(k, new TypedObject(v));
                }
            }
        );
    }

    /**
     * Gets the specified top-level field from the object.
     *
     * @param object The object to get from.
     * @param base The top-level field to get.
     * @return The value of the specified top-level field from the object or null if it does not exist.
     */
    protected abstract Object get(Object object, String base);

    /**
     * Gets the specified inner field from the object.
     *
     * @param object The object to get from.
     * @param field The inner field to get.
     * @return The value of the specified inner field from the object or null if it does not exist.
     */
    @SuppressWarnings("unchecked")
    protected Object getField(Object object, String field) {
        if (object instanceof List) {
            return ((List<Object>) object).get(Integer.parseInt(field));
        } else if (object instanceof Map) {
            return ((Map<String, Object>) object).get(field);
        }
        return null;
    }

    /**
     * Converts the field value with the given name and expected type into a {@link TypedObject}. If type checking is
     * enabled, it will check to make sure that the expected type matches the final type in the {@link TypedObject}.
     * This method is only used for non-record fields.
     *
     * @param name The name of the field.
     * @param type The expected type of the field.
     * @param value The {@link Serializable} value of the field.
     * @return A {@link TypedObject} wrapping the value.
     */
    protected TypedObject getTypedObject(String name, Type type, Serializable value) {
        if (!shouldTypeCheck) {
            return new TypedObject(type, value);
        }
        TypedObject object = new TypedObject(value);
        Type actual = object.getType();

        // If the object came back as an UNKNOWN container and it's empty and we have a schema, there's no need to fail
        actual = fixTypeIfEmpty(type, actual, value);
        if (type != actual) {
            throw new ClassCastException("Field " + name + " had type " + actual + " instead of the expected " + type);
        }
        return object;
    }

    /**
     * Extracts the specified field from the object. The field can contain map fields and list elements delimited by periods.
     *
     * @param object The object to extract from.
     * @param token The array of strings containing the split identifier of the field to get.
     * @return The value of the specified field from the object or null if it does not exist.
     */
    private Object extract(Object object, String[] token) {
        Object o = get(object, token[0]);
        for (int i = 1; o != null && i < token.length; i++) {
            o = getField(o, token[i]);
        }
        return o;
    }

    /**
     * Returns the expected type if value was actually an empty container. Otherwise, returns the actual type.
     *
     * @param expected The expected {@link Type} of the field from the schema.
     * @param actual The computed, actual {@link Type} of the field from the record.
     * @param value The actual value of field from the record.
     * @return Either the expected or actual type if the given value was empty.
     */
    private Type fixTypeIfEmpty(Type expected, Type actual, Serializable value) {
        switch (actual) {
            case UNKNOWN_LIST:
            case UNKNOWN_MAP_LIST:
                return ((List) value).isEmpty() ? expected : actual;
            case UNKNOWN_MAP:
            case UNKNOWN_MAP_MAP:
                return ((Map) value).isEmpty() ? expected : actual;
        }
        return actual;
    }

    /**
     * Creates a {@link JsonReader} from a file name or path.
     *
     * @param file The file name or path to read from.
     * @return A JsonReader reading from file.
     */
    private JsonReader getJsonReader(String file) {
        try {
            InputStream is = this.getClass().getResourceAsStream("/" + file);
            return is != null ? new JsonReader(new InputStreamReader(is)) : new JsonReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(file + " file not found.", e);
        }
    }

    /**
     * Creates a BulletRecordConverter instance using the specified class.
     *
     * @param config The configuration containing the BulletRecordConverter class name and other relevant settings.
     * @return A new instance of the specified BulletRecordConverter class.
     */
    public static BulletRecordConverter from(BulletDSLConfig config) {
        return config.loadConfiguredClass(BulletDSLConfig.RECORD_CONVERTER_CLASS_NAME);
    }
}
