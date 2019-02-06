/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.converter;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.record.BulletRecord;

import java.util.Map;
import java.util.Objects;

/**
 * MapBulletRecordConverter is used to convert {@code Map<String, Object>} to {@link BulletRecord}.
 * <br><br>
 * If a schema is not specified, maps are effectively flattened without any regard to type-safety.
 */
public class MapBulletRecordConverter extends BulletRecordConverter {

    private static final long serialVersionUID = -6592569189954638549L;

    /**
     * Constructs a MapBulletRecordConverter without a schema.
     *
     * @throws BulletDSLException if there is an error creating the converter.
     */
    public MapBulletRecordConverter() throws BulletDSLException {
        config = new BulletDSLConfig();
        build();
    }
    /**
     * Constructs a MapBulletRecordConverter from a given schema.
     *
     * @param schema A schema file that specifies the fields to extract and their types.
     * @throws BulletDSLException if there is an error creating the converter.
     */
    public MapBulletRecordConverter(String schema) throws BulletDSLException {
        Objects.requireNonNull(schema);
        config = new BulletDSLConfig();
        config.set(BulletDSLConfig.RECORD_CONVERTER_SCHEMA_FILE, schema);
        build();
    }

    /**
     * Constructs a MapBulletRecordConverter from a given configuration.
     *
     * @param bulletConfig The configuration that specifies the settings for a MapBulletRecordConverter.
     * @throws BulletDSLException if there is an error creating the converter.
     */
    public MapBulletRecordConverter(BulletConfig bulletConfig) throws BulletDSLException {
        // Copy settings from config.
        config = new BulletDSLConfig(bulletConfig);
        build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public BulletRecord convert(Object object, BulletRecord record) throws BulletDSLException {
        if (schema != null) {
            return super.convert(object, record);
        }
        // no bullet dsl schema
        Map<String, Object> map = (Map<String, Object>) object;
        map.forEach(
            (key, value) -> {
                if (value != null) {
                    record.forceSet(key, value);
                }
            }
        );
        return record;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object get(Object object, String base) {
        return ((Map<String, Object>) object).get(base);
    }
}
