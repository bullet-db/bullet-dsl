/*
 *  Copyright 2019, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.deserializer;

import com.yahoo.bullet.common.BulletConfig;

/**
 * A BulletDeserializer that does nothing. For convenience.
 */
public class IdentityDeserializer extends BulletDeserializer {

    private static final long serialVersionUID = 6761544669263842847L;

    /**
     * Constructor that takes a configuration containing the settings relevant for this deserializer.
     *
     * @param config The {@link BulletConfig} to use.
     */
    public IdentityDeserializer(BulletConfig config) {
        super(config);
    }

    @Override
    public Object deserialize(Object object) {
        return object;
    }
}
