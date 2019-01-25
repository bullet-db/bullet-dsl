/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Config;
import com.yahoo.bullet.common.Validator;
import com.yahoo.bullet.dsl.connector.KafkaConnector;
import com.yahoo.bullet.dsl.connector.PulsarConnector;
import com.yahoo.bullet.dsl.converter.POJOBulletRecordConverter;

import static com.yahoo.bullet.common.Validator.isImpliedBy;
import static java.util.function.Predicate.isEqual;

/**
 * Config class for BulletDSL.
 */
public class BulletDSLConfig extends BulletConfig {

    // BulletRecordConverter properties
    public static final String RECORD_CONVERTER_CLASS_NAME = "bullet.dsl.converter.class.name";
    public static final String RECORD_CONVERTER_SCHEMA_FILE = "bullet.dsl.converter.schema.file";
    public static final String RECORD_CONVERTER_POJO_CLASS_NAME = "bullet.dsl.converter.pojo.class.name";

    // BulletConnector properties
    public static final String CONNECTOR_CLASS_NAME = "bullet.dsl.connector.class.name";
    public static final String CONNECTOR_ASYNC_COMMIT_ENABLE = "bullet.dsl.connector.async.commit.enable";
    public static final String CONNECTOR_READ_TIMEOUT_MS = "bullet.dsl.connector.read.timeout.ms";

    public static final String CONNECTOR_KAFKA_NAMESPACE = "bullet.dsl.connector.kafka.";
    public static final String CONNECTOR_PULSAR_CLIENT_NAMESPACE = "bullet.dsl.connector.pulsar.client.";
    public static final String CONNECTOR_PULSAR_CONSUMER_NAMESPACE = "bullet.dsl.connector.pulsar.consumer.";

    public static final String CONNECTOR_KAFKA_BOOTSTRAP_SERVERS = "bullet.dsl.connector.kafka.bootstrap.servers";
    public static final String CONNECTOR_KAFKA_GROUP_ID = "bullet.dsl.connector.kafka.group.id";
    public static final String CONNECTOR_KAFKA_ENABLE_AUTO_COMMIT = "bullet.dsl.connector.kafka.enable.auto.commit";
    public static final String CONNECTOR_KAFKA_KEY_DESERIALIZER = "bullet.dsl.connector.kafka.key.deserializer";
    public static final String CONNECTOR_KAFKA_VALUE_DESERIALIZER = "bullet.dsl.connector.kafka.value.deserializer";
    public static final String CONNECTOR_KAFKA_TOPICS = "bullet.dsl.connector.kafka.topics";
    public static final String CONNECTOR_KAFKA_START_AT_END_ENABLE = "bullet.dsl.connector.kafka.start.at.end.enable";

    public static final String CONNECTOR_PULSAR_CLIENT_SERVICE_URL = "bullet.dsl.connector.pulsar.client.serviceUrl";
    public static final String CONNECTOR_PULSAR_CONSUMER_SUBSCRIPTION_NAME = "bullet.dsl.connector.pulsar.consumer.subscriptionName";
    public static final String CONNECTOR_PULSAR_CONSUMER_SUBSCRIPTION_TYPE = "bullet.dsl.connector.pulsar.consumer.subscriptionType";
    public static final String CONNECTOR_PULSAR_AUTH_ENABLE = "bullet.dsl.connector.pulsar.auth.enable";
    public static final String CONNECTOR_PULSAR_AUTH_PLUGIN_CLASS_NAME = "bullet.dsl.connector.pulsar.auth.plugin.class.name";
    public static final String CONNECTOR_PULSAR_AUTH_PARAMS_STRING = "bullet.dsl.connector.pulsar.auth.params.string";
    public static final String CONNECTOR_PULSAR_TOPICS = "bullet.dsl.connector.pulsar.topics";
    public static final String CONNECTOR_PULSAR_SCHEMA_CLASS_NAME = "bullet.dsl.connector.pulsar.schema.class.name";

    // pulsar.schema.type -> BYTES, STRING, JSON, AVRO, PROTOBUF,
    // pulsar.schema.of.class.name
    // pulsar.schema.custom.class.name


    // Class names
    public static final String POJO_BULLET_RECORD_CONVERTER_CLASS_NAME = POJOBulletRecordConverter.class.getName();

    public static final String KAFKA_CONNECTOR_CLASS_NAME = KafkaConnector.class.getName();
    public static final String PULSAR_CONNECTOR_CLASS_NAME = PulsarConnector.class.getName();

    // Defaults
    public static final String DEFAULT_DSL_CONFIGURATION = "bullet_dsl_defaults.yaml";
    public static final boolean DEFAULT_BULLET_CONNECTOR_ASYNC_COMMIT_ENABLE = true;
    public static final int DEFAULT_CONNECTOR_READ_TIMEOUT_MS = 0;
    public static final boolean DEFAULT_CONNECTOR_KAFKA_ENABLE_AUTO_COMMIT = true;
    public static final boolean DEFAULT_CONNECTOR_KAFKA_START_AT_END_ENABLE = false;
    public static final String DEFAULT_CONNECTOR_PULSAR_CONSUMER_SUBSCRIPTION_TYPE = "Shared";
    public static final boolean DEFAULT_CONNECTOR_PULSAR_AUTH_ENABLE = false;

    private static final Validator VALIDATOR = BulletConfig.getValidator();

    static {
        // BulletRecordConverter validation
        VALIDATOR.define(RECORD_CONVERTER_CLASS_NAME)
                 .checkIf(Validator::isClassName)
                 .unless(Validator::isNull)
                 .orFail();
        VALIDATOR.define(RECORD_CONVERTER_SCHEMA_FILE)
                 .checkIf(Validator::isString)
                 .unless(Validator::isNull)
                 .orFail();
        VALIDATOR.define(RECORD_CONVERTER_POJO_CLASS_NAME);
        VALIDATOR.relate("If using POJOBulletRecordConverter, a POJO class name must be specified.", RECORD_CONVERTER_CLASS_NAME, RECORD_CONVERTER_POJO_CLASS_NAME)
                 .checkIf(isImpliedBy(isEqual(POJO_BULLET_RECORD_CONVERTER_CLASS_NAME), Validator::isClassName))
                 .orFail();

        // BulletConnector validation
        VALIDATOR.define(CONNECTOR_CLASS_NAME)
                 .checkIf(Validator::isClassName)
                 .unless(Validator::isNull)
                 .orFail();
        VALIDATOR.define(CONNECTOR_ASYNC_COMMIT_ENABLE)
                 .checkIf(Validator::isBoolean)
                 .defaultTo(DEFAULT_BULLET_CONNECTOR_ASYNC_COMMIT_ENABLE);
        VALIDATOR.define(CONNECTOR_READ_TIMEOUT_MS)
                 .checkIf(Validator::isPositiveInt)
                 .unless(isEqual(0))
                 .defaultTo(DEFAULT_CONNECTOR_READ_TIMEOUT_MS);

        // KafkaConnector validation
        VALIDATOR.define(CONNECTOR_KAFKA_TOPICS);
        VALIDATOR.relate("If using KafkaConnector, a list of topic names must be specified.", CONNECTOR_CLASS_NAME, CONNECTOR_KAFKA_TOPICS)
                 .checkIf(isImpliedBy(isEqual(KAFKA_CONNECTOR_CLASS_NAME), Validator::isNonEmptyList))
                 .orFail();
        VALIDATOR.define(CONNECTOR_KAFKA_BOOTSTRAP_SERVERS);
        VALIDATOR.relate("If using KafkaConnector, bootstrap servers must be specified.", CONNECTOR_CLASS_NAME, CONNECTOR_KAFKA_BOOTSTRAP_SERVERS)
                 .checkIf(isImpliedBy(isEqual(KAFKA_CONNECTOR_CLASS_NAME), Validator::isString))
                 .orFail();
        VALIDATOR.define(CONNECTOR_KAFKA_GROUP_ID);
        VALIDATOR.relate("If using KafkaConnector, a group id must be specified.", CONNECTOR_CLASS_NAME, CONNECTOR_KAFKA_GROUP_ID)
                 .checkIf(isImpliedBy(isEqual(KAFKA_CONNECTOR_CLASS_NAME), Validator::isString))
                 .orFail();
        VALIDATOR.define(CONNECTOR_KAFKA_KEY_DESERIALIZER);
        VALIDATOR.relate("If using KafkaConnector, a key deserializer must be specified.", CONNECTOR_CLASS_NAME, CONNECTOR_KAFKA_KEY_DESERIALIZER)
                 .checkIf(isImpliedBy(isEqual(KAFKA_CONNECTOR_CLASS_NAME), Validator::isClassName))
                 .orFail();
        VALIDATOR.define(CONNECTOR_KAFKA_VALUE_DESERIALIZER);
        VALIDATOR.relate("If using KafkaConnector, a value deserializer must be specified.", CONNECTOR_CLASS_NAME, CONNECTOR_KAFKA_VALUE_DESERIALIZER)
                 .checkIf(isImpliedBy(isEqual(KAFKA_CONNECTOR_CLASS_NAME), Validator::isClassName))
                 .orFail();
        VALIDATOR.define(CONNECTOR_KAFKA_ENABLE_AUTO_COMMIT)
                 .checkIf(Validator::isBoolean)
                 .defaultTo(DEFAULT_CONNECTOR_KAFKA_ENABLE_AUTO_COMMIT);
        VALIDATOR.define(CONNECTOR_KAFKA_START_AT_END_ENABLE)
                 .checkIf(Validator::isBoolean)
                 .defaultTo(DEFAULT_CONNECTOR_KAFKA_START_AT_END_ENABLE);

        // PulsarConnector validation
        VALIDATOR.define(CONNECTOR_PULSAR_TOPICS);
        VALIDATOR.relate("If using PulsarConnector, a list of topic names must be specified.", CONNECTOR_CLASS_NAME, CONNECTOR_PULSAR_TOPICS)
                 .checkIf(isImpliedBy(isEqual(PULSAR_CONNECTOR_CLASS_NAME), Validator::isNonEmptyList))
                 .orFail();
        VALIDATOR.define(CONNECTOR_PULSAR_SCHEMA_CLASS_NAME);
        VALIDATOR.relate("If using PulsarConnector, schema class name must be specified.", CONNECTOR_CLASS_NAME, CONNECTOR_PULSAR_SCHEMA_CLASS_NAME)
                 .checkIf(isImpliedBy(isEqual(PULSAR_CONNECTOR_CLASS_NAME), Validator::isClassName))
                 .orFail();
        VALIDATOR.define(CONNECTOR_PULSAR_CLIENT_SERVICE_URL);
        VALIDATOR.relate("If using PulsarConnector, a service url must be specified.", CONNECTOR_CLASS_NAME, CONNECTOR_PULSAR_CLIENT_SERVICE_URL)
                 .checkIf(isImpliedBy(isEqual(PULSAR_CONNECTOR_CLASS_NAME), Validator::isString))
                 .orFail();
        VALIDATOR.define(CONNECTOR_PULSAR_CONSUMER_SUBSCRIPTION_NAME);
        VALIDATOR.relate("If using PulsarConnector, a subscription name must be specified.", CONNECTOR_CLASS_NAME, CONNECTOR_PULSAR_CONSUMER_SUBSCRIPTION_NAME)
                 .checkIf(isImpliedBy(isEqual(PULSAR_CONNECTOR_CLASS_NAME), Validator::isString))
                 .orFail();
        VALIDATOR.define(CONNECTOR_PULSAR_CONSUMER_SUBSCRIPTION_TYPE)
                 .checkIf(Validator::isString)
                 .defaultTo(DEFAULT_CONNECTOR_PULSAR_CONSUMER_SUBSCRIPTION_TYPE);
        VALIDATOR.define(CONNECTOR_PULSAR_AUTH_ENABLE)
                 .checkIf(Validator::isBoolean)
                 .defaultTo(DEFAULT_CONNECTOR_PULSAR_AUTH_ENABLE);
        VALIDATOR.define(CONNECTOR_PULSAR_AUTH_PLUGIN_CLASS_NAME);
        VALIDATOR.relate("If Pulsar authentication is enabled, authentication class name must be specified.", CONNECTOR_PULSAR_AUTH_ENABLE, CONNECTOR_PULSAR_AUTH_PLUGIN_CLASS_NAME)
                 .checkIf(isImpliedBy(Validator::isTrue, Validator::isClassName))
                 .orFail();
        VALIDATOR.define(CONNECTOR_PULSAR_AUTH_PARAMS_STRING)
                 .checkIf(Validator::isString)
                 .unless(Validator::isNull)
                 .orFail();
    }

    /**
     * Constructor that loads the defaults.
     */
    public BulletDSLConfig() {
        super(DEFAULT_DSL_CONFIGURATION);
        VALIDATOR.validate(this);
    }

    /**
     * Constructor that loads specific file augmented with defaults.
     *
     * @param file The YAML file to load.
     */
    public BulletDSLConfig(String file) {
        this(new Config(file));
    }

    /**
     * Constructor that loads another configuration and augments it with defaults.
     *
     * @param other The other configuration to wrap.
     */
    public BulletDSLConfig(Config other) {
        super(DEFAULT_DSL_CONFIGURATION);
        merge(other);
        VALIDATOR.validate(this);
    }

    @Override
    public BulletConfig validate() {
        super.validate();
        VALIDATOR.validate(this);
        return this;
    }
}
