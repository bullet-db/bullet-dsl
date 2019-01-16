/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.connector;

import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;

import java.io.Serializable;
import java.util.List;

/**
 * A BulletConnector is used to read objects from a data source such as Kafka or Pulsar. Connectors should extend this
 * class and expect configuration though {@link BulletDSLConfig}.
 */
public abstract class BulletConnector implements AutoCloseable, Serializable {

    protected BulletDSLConfig config;

    /**
     * Initializes the BulletConnector. Must be called before read().
     *
     * @throws BulletDSLException if there is an initialization error.
     */
    public abstract void initialize() throws BulletDSLException;

    /**
     * Reads and deserializes messages from a data source. A timeout duration can be set in the configuration.
     *
     * @return A list of deserialized objects.
     * @throws BulletDSLException if there is a connection or reading error.
     */
    public abstract List<Object> read() throws BulletDSLException;

    /**
     * Creates a BulletConnector instance using the specified class.
     *
     * @param config The configuration containing the BulletConnector class name and other relevant settings.
     * @return A new instance of the specified BulletConnector class.
     */
    public static BulletConnector from(BulletDSLConfig config) {
        return config.loadConfiguredClass(BulletDSLConfig.CONNECTOR_CLASS_NAME);
    }
}
