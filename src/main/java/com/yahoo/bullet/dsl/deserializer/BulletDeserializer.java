/*
 *  Copyright 2019, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.deserializer;

import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;

import java.io.Serializable;

/**
 * Super cool. Does cool stuff.
 */
public abstract class BulletDeserializer implements Serializable {

    protected BulletDSLConfig config;

    /**
     * Takes in. spits out.
     *
     * @param object
     * @return
     * @throws BulletDSLException
     */
    public abstract Object deserialize(Object object) throws BulletDSLException;

    /**
     * Creates a BulletConnector instance using the specified class.
     *
     * @param config The configuration containing the BulletConnector class name and other relevant settings.
     * @return A new instance of the specified BulletConnector class.
     */
    public static BulletDeserializer from(BulletDSLConfig config) {
        return config.loadConfiguredClass(BulletDSLConfig.DESERIALIZER_CLASS_NAME);
    }
}
