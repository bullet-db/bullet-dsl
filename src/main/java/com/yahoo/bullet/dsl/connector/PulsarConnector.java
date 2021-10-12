/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.connector;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A {@link BulletConnector} that reads and deserializes messages from Pulsar.
 */
@Slf4j
public class PulsarConnector extends BulletConnector {

    private static final long serialVersionUID = 2958805692867790602L;

    // Exposed for tests
    @Getter(AccessLevel.PACKAGE)
    private transient PulsarClient client;

    @Setter(AccessLevel.PACKAGE)
    private transient Consumer<Object> consumer;

    private boolean asyncCommit;
    private int timeout;

    /**
     * Constructs a PulsarConnector from a given configuration.
     *
     * @param bulletConfig The configuration that specifies the settings for a PulsarConnector.
     */
    @SuppressWarnings("unchecked")
    public PulsarConnector(BulletConfig bulletConfig) {
        super(bulletConfig);
        asyncCommit = config.getAs(BulletDSLConfig.CONNECTOR_ASYNC_COMMIT_ENABLE, Boolean.class);
        timeout = config.getAs(BulletDSLConfig.CONNECTOR_READ_TIMEOUT_MS, Number.class).intValue();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void initialize() throws BulletDSLException {
        Map<String, Object> clientConf = config.getAllWithPrefix(Optional.empty(), BulletDSLConfig.CONNECTOR_PULSAR_CLIENT_NAMESPACE, true);
        Map<String, Object> consumerConf = config.getAllWithPrefix(Optional.empty(), BulletDSLConfig.CONNECTOR_PULSAR_CONSUMER_NAMESPACE, true);
        List<String> topics = config.getAs(BulletDSLConfig.CONNECTOR_PULSAR_TOPICS, List.class);
        Boolean authEnable = config.getAs(BulletDSLConfig.CONNECTOR_PULSAR_AUTH_ENABLE, Boolean.class);
        String authPluginClassName = config.getAs(BulletDSLConfig.CONNECTOR_PULSAR_AUTH_PLUGIN_CLASS_NAME, String.class);
        String authParamsString = config.getAs(BulletDSLConfig.CONNECTOR_PULSAR_AUTH_PARAMS_STRING, String.class);
        String schemaType = config.getAs(BulletDSLConfig.CONNECTOR_PULSAR_SCHEMA_TYPE, String.class);
        String schemaClassName = config.getAs(BulletDSLConfig.CONNECTOR_PULSAR_SCHEMA_CLASS_NAME, String.class);

        Schema schema = getSchema(schemaType, schemaClassName);

        client = getPulsarClient(clientConf, authEnable, authPluginClassName, authParamsString);

        try {
            consumer = client.newConsumer(schema).loadConf(consumerConf).topics(topics).subscribe();
        } catch (Exception e) {
            throw new BulletDSLException("Could not create Pulsar consumer.", e);
        }
    }

    // Exposed for testing
    PulsarClient getPulsarClient(Map<String, Object> clientConf, Boolean authEnable, String authPluginClassName, String authParamsString) throws BulletDSLException {
        try {
            ClientBuilder builder = PulsarClient.builder().loadConf(clientConf);
            if (authEnable) {
                builder.authentication(authPluginClassName, authParamsString);
            }
            return builder.build();
        } catch (Exception e) {
            throw new BulletDSLException("Could not create Pulsar client.", e);
        }
    }

    @Override
    public List<Object> read() throws BulletDSLException {
        List<Object> objects = new ArrayList<>();
        Message<Object> message;
        while ((message = getMessage()) != null) {
            objects.add(message.getValue());
            acknowledge(message);
        }
        return objects;
    }

    @Override
    public void close() {
        consumer.closeAsync();
        client.closeAsync();
    }

    private Message<Object> getMessage() throws BulletDSLException {
        try {
            return consumer.receive(timeout, TimeUnit.MILLISECONDS);
        } catch (PulsarClientException e) {
            throw new BulletDSLException("Could not read from consumer.", e);
        }
    }

    private void acknowledge(Message<Object> message) throws BulletDSLException {
        if (asyncCommit) {
            consumer.acknowledgeAsync(message);
        } else {
            try {
                consumer.acknowledge(message);
            } catch (PulsarClientException e) {
                throw new BulletDSLException("Could not acknowledge message.", e);
            }
        }
    }

    private Schema getSchema(String schemaType, String schemaClassName) throws BulletDSLException {
        try {
            Class schemaClass = schemaClassName != null ? Class.forName(schemaClassName) : null;
            switch (schemaType) {
                case "BYTES":
                    return Schema.BYTES;
                case "STRING":
                    return Schema.STRING;
                case "JSON":
                    return Schema.JSON(schemaClass);
                case "AVRO":
                    return Schema.AVRO(schemaClass);
                case "PROTOBUF":
                    return Schema.PROTOBUF(schemaClass); // class must extend GeneratedMessageV3
                case "CUSTOM":
                    Constructor<Schema> c = schemaClass.getDeclaredConstructor();
                    return c.newInstance();
            }
        } catch (Exception e) {
            throw new BulletDSLException("Could not create Pulsar schema.", e);
        }
        throw new BulletDSLException("Pulsar schema type must be one of: " + BulletDSLConfig.PULSAR_SCHEMA_TYPES);
    }
}
