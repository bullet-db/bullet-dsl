/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.connector;

import com.yahoo.bullet.dsl.BulletDSLConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BulletConnectorTest {

    private BulletDSLConfig config;

    @BeforeMethod
    public void init() {
        config = new BulletDSLConfig("test_connector_config.yaml");
    }

    @Test
    public void testFromKafkaConnector() throws Exception {
        config.set(BulletDSLConfig.CONNECTOR_CLASS_NAME, "com.yahoo.bullet.dsl.connector.KafkaConnector");

        BulletConnector connector = BulletConnector.from(config);
        Assert.assertTrue(connector instanceof KafkaConnector);

        connector.initialize();

        KafkaConsumer<Object, Object> consumer = Mockito.mock(KafkaConsumer.class);

        Map<TopicPartition, List<ConsumerRecord<Object, Object>>> map = new HashMap<>();
        map.put(new TopicPartition("mytopic", 0), Collections.emptyList());
        ConsumerRecords<Object, Object> buffer = new ConsumerRecords<>(map);
        Mockito.doReturn(buffer).when(consumer).poll(Mockito.any());

        ((KafkaConnector) connector).setConsumer(consumer);

        List<Object> objects = connector.read();
        Assert.assertEquals(objects.size(), 0);

        connector.close();
    }

    @Test
    public void testFromPulsarConnector() throws Exception {
        config.set(BulletDSLConfig.CONNECTOR_CLASS_NAME, "com.yahoo.bullet.dsl.connector.PulsarConnector");

        BulletConnector connector = BulletConnector.from(config);
        Assert.assertTrue(connector instanceof PulsarConnector);

        ConsumerBuilder<Object> consumerBuilder = Mockito.mock(ConsumerBuilder.class);
        Consumer<Object> consumer = Mockito.mock(Consumer.class);

        Mockito.doReturn(consumer).when(consumerBuilder).subscribe();
        Mockito.when(consumer.receive(Mockito.anyInt(), Mockito.any())).thenReturn(null);

        ((PulsarConnector) connector).setConsumerBuilder(consumerBuilder);

        connector.initialize();

        List<Object> objects = connector.read();
        Assert.assertEquals(objects.size(), 0);

        connector.close();
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testThrow() {
        BulletConnector.from(config);
    }
}
