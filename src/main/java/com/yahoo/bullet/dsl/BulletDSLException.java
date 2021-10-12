/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl;

/**
 * Exception to be thrown if there is an error in BulletConnector or BulletRecordConverter.
 */
public class BulletDSLException extends Exception {

    private static final long serialVersionUID = -4845209101137527167L;

    /**
     * Constructor to initialize BulletDSLException with a message.
     *
     * @param message The error message to be associated with this BulletDSLException.
     */
    public BulletDSLException(String message) {
        super(message);
    }

    /**
     * Constructor to initialize BulletDSLException with a message and a {@link Throwable} cause.
     *
     * @param message The error message to be associated with this BulletDSLException.
     * @param cause The reason for this BulletDSLException.
     */
    public BulletDSLException(String message, Throwable cause) {
        super(message, cause);
    }
}
