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
import java.util.Map;
import java.util.Optional;

/**
 * A {@link BulletConnector} that reads and deserializes messages from Kafka.
 */
@Slf4j
public class KafkaConnector extends BulletConnector {

    // Exposed for tests
    @Setter(AccessLevel.PACKAGE)
    private KafkaConsumer<Object, Object> consumer;

    private List<String> topics;
    private boolean startAtEnd;
    private boolean autoCommit;
    private boolean asyncCommit;
    private Duration timeout;
    private Map<String, Object> properties;

    /**
     * Constructs a KafkaConnector from a given configuration.
     *
     * @param config The configuration that specifies the settings for a KafkaConnector.
     */
    public KafkaConnector(BulletConfig config) {
        // Copy settings from config.
        this.config = new BulletDSLConfig(config);
        this.topics = this.config.getAs(BulletDSLConfig.CONNECTOR_KAFKA_TOPICS, List.class);
        this.startAtEnd = this.config.getAs(BulletDSLConfig.CONNECTOR_KAFKA_START_AT_END_ENABLE, Boolean.class);
        this.autoCommit = this.config.getAs(BulletDSLConfig.CONNECTOR_KAFKA_ENABLE_AUTO_COMMIT, Boolean.class);
        this.asyncCommit = this.config.getAs(BulletDSLConfig.CONNECTOR_ASYNC_COMMIT_ENABLE, Boolean.class);
        this.timeout = Duration.ofMillis(this.config.getAs(BulletDSLConfig.CONNECTOR_READ_TIMEOUT_MS, Number.class).longValue());
        this.properties = this.config.getAllWithPrefix(Optional.empty(), BulletDSLConfig.CONNECTOR_KAFKA_NAMESPACE, true);
    }

    @Override
    public void initialize() {
        consumer = new KafkaConsumer<>(properties);
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
