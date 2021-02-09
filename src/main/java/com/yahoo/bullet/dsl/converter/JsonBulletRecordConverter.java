/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.converter;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.record.BulletRecord;

import java.io.Serializable;
import java.util.Map;

/**
 * JsonBulletRecordConverter is used to convert {@link String} json to {@link BulletRecord}. It uses Gson to convert
 * the json to a map which is then converted to a {@link BulletRecord}.
 * <br><br>
 * If a schema is not specified, the json is effectively flattened without any regard to type-safety.
 */
public class JsonBulletRecordConverter extends MapBulletRecordConverter {

    private static final long serialVersionUID = 8262353280806978091L;
    private static final Gson GSON = new Gson();

    public JsonBulletRecordConverter() throws BulletDSLException {
        super();
    }

    public JsonBulletRecordConverter(String schema) throws BulletDSLException {
        super(schema);
    }

    public JsonBulletRecordConverter(BulletConfig bulletConfig) throws BulletDSLException {
        super(bulletConfig);
    }

    @Override
    public BulletRecord convert(Object object, BulletRecord record) throws BulletDSLException {
        String json = (String) object;
        Map<String, Serializable> map = GSON.fromJson(json, new TypeToken<Map<String, Object>>() { }.getType());
        return super.convert(map, record);
    }
}
