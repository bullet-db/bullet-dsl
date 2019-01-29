/*
 *  Copyright 2018, Oath Inc.
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
import org.apache.pulsar.client.api.ConsumerBuilder;
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

    // Exposed for tests
    @Getter(AccessLevel.PACKAGE)
    private PulsarClient client;

    @Setter(AccessLevel.PACKAGE)
    private ConsumerBuilder<Object> consumerBuilder;

    @Setter(AccessLevel.PACKAGE)
    private Consumer<Object> consumer;

    private boolean asyncCommit;
    private int timeout;

    /**
     * Constructs a PulsarConnector from a given configuration.
     *
     * @param config The configuration that specifies the settings for a PulsarConnector.
     * @throws PulsarClientException If there is an error creating the {@link PulsarClient}.
     */
    @SuppressWarnings("unchecked")
    public PulsarConnector(BulletConfig config) throws Exception {
        // Copy settings from config.
        this.config = new BulletDSLConfig(config);
        this.asyncCommit = this.config.getAs(BulletDSLConfig.CONNECTOR_ASYNC_COMMIT_ENABLE, Boolean.class);
        this.timeout = this.config.getAs(BulletDSLConfig.CONNECTOR_READ_TIMEOUT_MS, Number.class).intValue();

        Map<String, Object> clientConf = this.config.getAllWithPrefix(Optional.empty(), BulletDSLConfig.CONNECTOR_PULSAR_CLIENT_NAMESPACE, true);
        Map<String, Object> consumerConf = this.config.getAllWithPrefix(Optional.empty(), BulletDSLConfig.CONNECTOR_PULSAR_CONSUMER_NAMESPACE, true);
        List<String> topics = this.config.getAs(BulletDSLConfig.CONNECTOR_PULSAR_TOPICS, List.class);
        Boolean authEnable = this.config.getAs(BulletDSLConfig.CONNECTOR_PULSAR_AUTH_ENABLE, Boolean.class);
        String authPluginClassName = this.config.getAs(BulletDSLConfig.CONNECTOR_PULSAR_AUTH_PLUGIN_CLASS_NAME, String.class);
        String authParamsString = this.config.getAs(BulletDSLConfig.CONNECTOR_PULSAR_AUTH_PARAMS_STRING, String.class);
        String schemaType = this.config.getAs(BulletDSLConfig.CONNECTOR_PULSAR_SCHEMA_TYPE, String.class);
        String schemaClassName = this.config.getAs(BulletDSLConfig.CONNECTOR_PULSAR_SCHEMA_CLASS_NAME, String.class);

        Schema schema;
        Class schemaClass = Class.forName(schemaClassName);

        switch (schemaType) {
            case "BYTES":
                schema = Schema.BYTES;
                break;
            case "STRING":
                schema = Schema.STRING;
                break;
            case "JSON":
                schema = Schema.JSON(schemaClass);
                break;
            case "AVRO":
                schema = Schema.AVRO(schemaClass);
                break;
            case "PROTOBUF":
                schema = Schema.PROTOBUF(schemaClass); // class must extend GeneratedMessageV3
                break;
            case "CUSTOM":
                try {
                    Constructor<Schema> c = schemaClass.getDeclaredConstructor(BulletDSLConfig.class);
                    schema = c.newInstance(this.config);
                } catch (Exception e) {
                    Constructor<Schema> c = schemaClass.getDeclaredConstructor();
                    schema = c.newInstance();
                }
                break;
            default:
                throw new BulletDSLException("Could not create Pulsar Schema.");
        }

        ClientBuilder builder = PulsarClient.builder().loadConf(clientConf);
        if (authEnable) {
            builder.authentication(authPluginClassName, authParamsString);
        }
        client = builder.build();
        consumerBuilder = client.newConsumer(schema).loadConf(consumerConf).topics(topics);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void initialize() throws BulletDSLException {
        try {
            consumer = consumerBuilder.subscribe();
        } catch (Exception e) {
            throw new BulletDSLException("Could not create consumer.", e);
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
}
