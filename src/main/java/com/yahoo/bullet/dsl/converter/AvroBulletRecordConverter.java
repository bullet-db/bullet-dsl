/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.converter;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.dsl.schema.BulletRecordField;
import com.yahoo.bullet.record.BulletRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;
import java.util.Objects;

/**
 * AvroBulletRecordConverter converts an Avro record into a BulletRecord. The Avro record must use {@link String} as its
 * string type.
 * <br><br>
 * If a schema is not specified, avro records are effectively flattened without any regard to type-safety.
 * <br><br>
 * Note, this class is not related to {@link com.yahoo.bullet.record.AvroBulletRecord}.
 */
@Slf4j
public class AvroBulletRecordConverter extends BulletRecordConverter {

    /**
     * Constructs an AvroBulletRecordConverter without a schema.
     *
     * @throws BulletDSLException if there is an error creating the converter.
     */
    public AvroBulletRecordConverter() throws BulletDSLException {
        config = new BulletDSLConfig();
        build();
    }

    /**
     * Constructs an AvroBulletRecordConverter from a given schema.
     *
     * @param schema A schema file that specifies the fields to extract and their types.
     * @throws BulletDSLException if there is an error creating the converter.
     */
    public AvroBulletRecordConverter(String schema) throws BulletDSLException {
        Objects.requireNonNull(schema);
        config = new BulletDSLConfig();
        config.set(BulletDSLConfig.RECORD_CONVERTER_SCHEMA_FILE, schema);
        build();
    }

    /**
     * Constructs an AvroBulletRecordConverter from a given configuration.
     *
     * @param config The configuration that specifies the settings for an AvroBulletRecordConverter.
     * @throws BulletDSLException if there is an error creating the converter.
     */
    public AvroBulletRecordConverter(BulletConfig config) throws BulletDSLException {
        // Copy settings from config.
        this.config = new BulletDSLConfig(config);
        build();
    }

    @Override
    protected BulletRecordConverter build() throws BulletDSLException {
        super.build();

        // check for specified avro record schema file option
        // try to get schema if there and fail if can't get it

        // check for specified avro record class
        // try to get schema if there and fail if can't get it

        // check for deserialize byte[] option
        // if there, check for schema and fail if cant get it
        // set up decoding


        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public BulletRecord convert(Object object, BulletRecord record) throws BulletDSLException {
        // if deserialize
        // deserialize and then set object = decode((byte[]) object);


        if (schema != null) {
            return super.convert(object, record);
        }
        // no bullet dsl schema
        GenericRecord avro = (GenericRecord) object;
        for (Schema.Field field : avro.getSchema().getFields()) {
            Object value = avro.get(field.pos());
            if (value != null) {
                record.forceSet(field.name(), value);
            }
        }
        return record;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void setField(BulletRecordField field, Object value, BulletRecord record) {
        switch (field.getType()) {
            case RECORD:
                if (value instanceof Map) {
                    flattenMap((Map<String, Object>) value, record);
                } else {
                    flattenRecord((GenericRecord) value, record);
                }
                break;
            default:
                super.setField(field, value, record);
        }
    }

    private void flattenRecord(GenericRecord genericRecord, BulletRecord record) {
        for (Schema.Field field : genericRecord.getSchema().getFields()) {
            String key = field.name();
            Object value = genericRecord.get(field.pos());
            if (value != null) {
                record.forceSet(key, value);
            }
        }
    }

    @Override
    protected Object get(Object object, String base) {
        return ((GenericRecord) object).get(base);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object getField(Object object, String field) {
        if (object instanceof GenericRecord) {
            return ((GenericRecord) object).get(field);
        }
        return super.getField(object, field);
    }
}
