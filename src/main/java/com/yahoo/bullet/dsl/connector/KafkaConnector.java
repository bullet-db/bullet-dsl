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
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * A {@link BulletConnector} that reads and deserializes messages from Kafka.
 */
@Slf4j
public class KafkaConnector extends BulletConnector {

    private static final long serialVersionUID = -256168979644903950L;

    // Exposed for tests
    @Setter(AccessLevel.PACKAGE)
    private KafkaConsumer<Object, Object> consumer;

    private List<String> topics;
    private boolean startAtEnd;
    private boolean autoCommit;
    private boolean asyncCommit;
    private Duration timeout;

    /**
     * Constructs a KafkaConnector from a given configuration.
     *
     * @param bulletConfig The configuration that specifies the settings for a KafkaConnector.
     */
    public KafkaConnector(BulletConfig bulletConfig) {
        // Copy settings from config.
        config = new BulletDSLConfig(bulletConfig);
        topics = config.getAs(BulletDSLConfig.CONNECTOR_KAFKA_TOPICS, List.class);
        startAtEnd = config.getAs(BulletDSLConfig.CONNECTOR_KAFKA_START_AT_END_ENABLE, Boolean.class);
        autoCommit = config.getAs(BulletDSLConfig.CONNECTOR_KAFKA_ENABLE_AUTO_COMMIT, Boolean.class);
        asyncCommit = config.getAs(BulletDSLConfig.CONNECTOR_ASYNC_COMMIT_ENABLE, Boolean.class);
        timeout = Duration.ofMillis(config.getAs(BulletDSLConfig.CONNECTOR_READ_TIMEOUT_MS, Number.class).longValue());
    }

    @Override
    public void initialize() {
        consumer = new KafkaConsumer<>(config.getAllWithPrefix(Optional.empty(), BulletDSLConfig.CONNECTOR_KAFKA_NAMESPACE, true));
        consumer.subscribe(topics);
        if (startAtEnd) {
            consumer.seekToEnd(Collections.emptyList());
        }
        consumer.poll(Duration.ZERO);
    }

    @Override
    public List<Object> read() throws BulletDSLException {
        ConsumerRecords<Object, Object> buffer;
        try {
            buffer = consumer.poll(timeout);
        } catch (KafkaException e) {
            throw new BulletDSLException("Could not read from consumer.", e);
        }
        List<Object> objects = new ArrayList<>();
        buffer.forEach(record -> objects.add(record.value()));
        if (!autoCommit) {
            commit();
        }
        return objects;
    }

    @Override
    public void close() {
        consumer.close();
    }

    private void commit() {
        if (asyncCommit) {
            consumer.commitAsync();
        } else {
            consumer.commitSync();
        }
    }
}
