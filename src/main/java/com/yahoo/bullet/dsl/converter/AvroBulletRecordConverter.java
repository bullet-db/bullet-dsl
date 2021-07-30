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
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * AvroBulletRecordConverter converts an Avro record into a BulletRecord. The Avro record must use {@link String} as its
 * string type.
 * <br><br>
 * If a schema is not specified, avro records are effectively flattened without any regard to type-safety.
 * <br><br>
 * Note, this class is not related to {@link com.yahoo.bullet.record.avro.TypedAvroBulletRecord} or
 * {@link com.yahoo.bullet.record.avro.UntypedAvroBulletRecord}.
 */
@Slf4j
public class AvroBulletRecordConverter extends BulletRecordConverter {

    private static final long serialVersionUID = -5066600942303615002L;
    protected boolean runStringFixer;

    /**
     * Constructs an AvroBulletRecordConverter without a schema.
     *
     * @throws BulletDSLException if there is an error creating the converter.
     */
    public AvroBulletRecordConverter() throws BulletDSLException {
        super(null);
        build();
    }

    /**
     * Constructs an AvroBulletRecordConverter from a given schema.
     *
     * @param schema A schema file that specifies the fields to extract and their types.
     * @throws BulletDSLException if there is an error creating the converter.
     */
    public AvroBulletRecordConverter(String schema) throws BulletDSLException {
        super(null);
        Objects.requireNonNull(schema);
        config.set(BulletDSLConfig.RECORD_CONVERTER_SCHEMA_FILE, schema);
        config.validate();
        build();
    }

    /**
     * Constructs an AvroBulletRecordConverter from a given configuration.
     *
     * @param bulletConfig The configuration that specifies the settings for an AvroBulletRecordConverter.
     * @throws BulletDSLException if there is an error creating the converter.
     */
    public AvroBulletRecordConverter(BulletConfig bulletConfig) throws BulletDSLException {
        super(bulletConfig);
        build();
    }

    @Override
    protected BulletRecordConverter build() throws BulletDSLException {
        BulletRecordConverter converter = super.build();
        runStringFixer = config.getAs(BulletDSLConfig.RECORD_CONVERTER_AVRO_STRING_TYPE_FIX_ENABLE, Boolean.class);
        return converter;
    }

    @Override
    @SuppressWarnings("unchecked")
    public BulletRecord convert(Object object, BulletRecord record) throws BulletDSLException {
        if (schema != null) {
            return super.convert(object, record);
        }
        // No Bullet DSL schema
        GenericRecord avro = (GenericRecord) object;
        return convertGenericRecord(avro, avro.getSchema(), record);
    }

    /**
     * Converts a {@link GenericRecord} with a {@link Schema} into the provided {@link BulletRecord}.
     *
     * @param genericRecord The {@link GenericRecord} to convert.
     * @param schema The {@link Schema} of the {@link GenericRecord}.
     * @param record The {@link BulletRecord} to place the fields into.
     * @return The {@link BulletRecord} with the added fields.
     */
    protected BulletRecord convertGenericRecord(GenericRecord genericRecord, Schema schema, BulletRecord record) {
        for (Schema.Field field : schema.getFields()) {
            Object datum = genericRecord.get(field.pos());
            if (datum != null) {
                Serializable value = runStringFixer ? fix(field.schema(), datum) : (Serializable) datum;
                record.typedSet(field.name(), new TypedObject(value));
            }
        }
        return record;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void setField(BulletRecordField field, Object value, BulletRecord record) {
        if (field.getType() == null) {
            if (value instanceof Map) {
                flattenMap((Map<String, Serializable>) value, record);
            } else {
                flattenRecord((GenericRecord) value, record);
            }
        } else {
            super.setField(field, value, record);
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

    private void flattenRecord(GenericRecord genericRecord, BulletRecord record) {
        for (Schema.Field field : genericRecord.getSchema().getFields()) {
            String key = field.name();
            Serializable value = (Serializable) genericRecord.get(field.pos());
            if (value != null) {
                record.typedSet(key, new TypedObject(value));
            }
        }
    }

    private Serializable fix(Schema fieldSchema, Object datum) {
        if (datum == null) {
            return null;
        }
        switch (fieldSchema.getType()) {
            case STRING:
                return datum.toString();
            case UNION:
                return fixUnion(fieldSchema.getTypes(), datum);
            case MAP:
                return fixMap(fieldSchema.getValueType(), (Map<CharSequence, Object>) datum);
            case ARRAY:
                return fixArray(fieldSchema.getElementType(), (List<Object>) datum);
        }
        return (Serializable) datum;
    }

    private Serializable fixUnion(List<Schema> types, Object value) {
        Serializable fixed = null;
        for (Schema schema : types) {
            Schema.Type type = schema.getType();
            if (type == Schema.Type.NULL) {
                continue;
            }
            // Use the first non null type that works
            try {
                fixed = fix(schema, value);
            } catch (Exception ignored) {
            }
        }
        return fixed;
    }

    private Serializable fixMap(Schema valueType, Map<CharSequence, Object> value) {
        HashMap<String, Object> map = new HashMap<>();
        value.forEach((k, v) -> map.put(k == null ? null : k.toString(), fix(valueType, v)));
        return map;
    }

    private Serializable fixArray(Schema elementType, List<Object> value) {
        ArrayList<Object> list = new ArrayList<>();
        value.forEach(e -> list.add(fix(elementType, e)));
        return list;
    }
}
