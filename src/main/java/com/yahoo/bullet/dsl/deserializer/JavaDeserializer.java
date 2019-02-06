/*
 *  Copyright 2019, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.deserializer;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.SerializerDeserializer;

/**
 * A {@link BulletDeserializer} that uses Java deserialization.
 */
public class JavaDeserializer extends BulletDeserializer {

    private static final long serialVersionUID = -8588983048490798720L;

    /**
     * Constructs a JavaDeserializer from a given (but unused) configuration. Required constructor.
     *
     * @param bulletConfig Not used.
     */
    public JavaDeserializer(BulletConfig bulletConfig) {
        super(bulletConfig);
    }

    @Override
    public Object deserialize(Object object) {
        return SerializerDeserializer.fromBytes((byte[]) object);
    }
}
