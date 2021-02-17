/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.converter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * JSONBulletRecordConverter is used to convert String JSON to {@link BulletRecord} instances. The JSON must be an
 * object containing the fields of the record.
 * <br><br>
 * If a schema is not specified, numeric types will default to {@link Double}. If a schema is provided, the appropriate
 * specified types will be used.
 */
public class JSONBulletRecordConverter extends MapBulletRecordConverter {

    private static final long serialVersionUID = -9133702879277054842L;
    private static final Gson GSON = new GsonBuilder().create();

    /**
     * Constructs a JSONBulletRecordConverter without a schema.
     *
     * @throws BulletDSLException if there is an error creating the converter.
     */
    public JSONBulletRecordConverter() throws BulletDSLException {
        super();
    }

    /**
     * Constructs a JSONBulletRecordConverter from a given schema.
     *
     * @param schema A schema file that specifies the fields to extract and their types.
     * @throws BulletDSLException if there is an error creating the converter.
     */
    public JSONBulletRecordConverter(String schema) throws BulletDSLException {
        super(schema);
    }

    /**
     * Constructs a JSONBulletRecordConverter from a given configuration.
     *
     * @param bulletConfig The configuration that specifies the settings for a JSONBulletRecordConverter.
     * @throws BulletDSLException if there is an error creating the converter.
     */
    public JSONBulletRecordConverter(BulletConfig bulletConfig) throws BulletDSLException  {
        super(bulletConfig);
    }

    @Override
    public BulletRecord convert(Object object, BulletRecord record) throws BulletDSLException {
        String json = (String) object;
        Map<String, Object> data = GSON.fromJson(json, new TypeToken<Map<String, Object>>() { }.getType());
        return super.convert(data, record);
    }

    @Override
    protected TypedObject getTypedObject(String name, Type type, Serializable value) {
        return super.getTypedObject(name, type, fixNumberType(type, value));
    }

    private static Serializable fixNumberType(Type expected, Serializable object) {
        // Cannot get nulls as it is already checked before getTypedObject is called in BulletRecordConverter#convert
        switch (expected) {
            case INTEGER:
                return toInt(object);
            case LONG:
                return toLong(object);
            case FLOAT:
                return toFloat(object);
            case INTEGER_MAP:
                return toNumberMap(object, JSONBulletRecordConverter::toInt);
            case LONG_MAP:
                return toNumberMap(object, JSONBulletRecordConverter::toLong);
            case FLOAT_MAP:
                return toNumberMap(object, JSONBulletRecordConverter::toFloat);
            case INTEGER_LIST:
                return toNumberList(object, JSONBulletRecordConverter::toInt);
            case LONG_LIST:
                return toNumberList(object, JSONBulletRecordConverter::toLong);
            case FLOAT_LIST:
                return toNumberList(object, JSONBulletRecordConverter::toFloat);
            case INTEGER_MAP_MAP:
                return toNumberMap(object, s -> toNumberMap(s, JSONBulletRecordConverter::toInt));
            case LONG_MAP_MAP:
                return toNumberMap(object, s -> toNumberMap(s, JSONBulletRecordConverter::toLong));
            case FLOAT_MAP_MAP:
                return toNumberMap(object, s -> toNumberMap(s, JSONBulletRecordConverter::toFloat));
            case INTEGER_MAP_LIST:
                return toNumberList(object, s -> toNumberMap(s, JSONBulletRecordConverter::toInt));
            case LONG_MAP_LIST:
                return toNumberList(object, s -> toNumberMap(s, JSONBulletRecordConverter::toLong));
            case FLOAT_MAP_LIST:
                return toNumberList(object, s -> toNumberMap(s, JSONBulletRecordConverter::toFloat));
        }
        return object;
    }

    private static Serializable toInt(Serializable primitive) {
        return primitive == null ? null : ((Number) primitive).intValue();
    }

    private static Serializable toLong(Serializable primitive) {
        return primitive == null ? null : ((Number) primitive).longValue();
    }

    private static Serializable toFloat(Serializable primitive) {
        return primitive == null ? null : ((Number) primitive).floatValue();
    }

    @SuppressWarnings("unchecked")
    private static Serializable toNumberMap(Serializable map, UnaryOperator<Serializable> mapper) {
        if (map != null) {
            Map<String, Serializable> asMap = (Map<String, Serializable>) map;
            asMap.replaceAll((k, v) -> mapper.apply(v));
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static Serializable toNumberList(Serializable list, UnaryOperator<Serializable> mapper) {
        if (list != null) {
            List<Serializable> asList = (List<Serializable>) list;
            asList.replaceAll(mapper);
        }
        return list;
    }
}
