/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.converter;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.dsl.schema.BulletRecordField;
import com.yahoo.bullet.dsl.schema.BulletRecordSchema;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;

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
 * configuration though {@link BulletDSLConfig}. If a {@link BulletRecordSchema} is provided, values will be typed casted
 * during conversion. If a schema is not provided, the level of type-checking is left to the implementation.
 */
public abstract class BulletRecordConverter implements Serializable {

    private BulletRecordProvider provider;
    protected BulletDSLConfig config;
    protected BulletRecordSchema schema;

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
     * Converts an object to a BulletRecord using given record. The default implementation assumes a valid schema.
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
        switch (field.getType()) {
            case BOOLEAN:
                record.setBoolean(field.getName(), (Boolean) value);
                break;
            case INTEGER:
                record.setInteger(field.getName(), (Integer) value);
                break;
            case LONG:
                record.setLong(field.getName(), (Long) value);
                break;
            case FLOAT:
                record.setFloat(field.getName(), (Float) value);
                break;
            case DOUBLE:
                record.setDouble(field.getName(), (Double) value);
                break;
            case STRING:
                record.setString(field.getName(), (String) value);
                break;
            case LIST:
            case LISTOFMAP:
                record.forceSet(field.getName(), (List<Object>) value);
                break;
            case MAP:
            case MAPOFMAP:
                record.forceSet(field.getName(), (Map<String, Object>) value);
                break;
            case RECORD:
                flattenMap((Map<String, Object>) value, record);
        }
    }

    /**
     * Takes the fields of a map and inserts them into a BulletRecord.
     *
     * @param mapRecord The map to take fields from.
     * @param record The BulletRecord to insert fields into.
     */
    protected void flattenMap(Map<String, Object> mapRecord, BulletRecord record) {
        mapRecord.forEach(
            (k, v) -> {
                if (v != null) {
                    record.forceSet(k, v);
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
