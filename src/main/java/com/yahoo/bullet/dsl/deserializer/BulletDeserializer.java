/*
 *  Copyright 2019, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.deserializer;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;

import java.io.Serializable;

/**
 * A BulletDeserializer is used to deserialize or transform output from a BulletConnector into input for a BulletRecordConverter, e.g.
 * from byte arrays to maps. Deserializers should extend this class and expect configuration though {@link BulletDSLConfig}.
 */
public abstract class BulletDeserializer implements Serializable {

    private static final long serialVersionUID = 3601804496002477644L;
    protected BulletDSLConfig config;

    /**
     * Constructor that takes a configuration containing the settings relevant for this deserializer.
     *
     * @param bulletConfig The {@link BulletConfig} to use.
     */
    public BulletDeserializer(BulletConfig bulletConfig) {
        this.config = new BulletDSLConfig(bulletConfig);
    }

    /**
     * Deserializes or transforms an object.
     *
     * @param object The object to deserialize or transform.
     * @return The deserialized or transformed object.
     * @throws BulletDSLException if there is a deserialization error.
     */
    public abstract Object deserialize(Object object) throws BulletDSLException;

    /**
     * Creates a BulletDeserializer instance using the specified class.
     *
     * @param config The configuration containing the BulletDeserializer class name and other relevant settings.
     * @return A new instance of the specified BulletDeserializer class.
     */
    public static BulletDeserializer from(BulletDSLConfig config) {
        return config.loadConfiguredClass(BulletDSLConfig.DESERIALIZER_CLASS_NAME);
    }
}
