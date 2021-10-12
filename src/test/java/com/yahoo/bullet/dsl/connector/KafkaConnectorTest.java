/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.connector;

import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaConnectorTest {

    private BulletDSLConfig config;

    @BeforeMethod
    public void init() {
        config = new BulletDSLConfig("test_connector_config.yaml");
    }

    @Test
    public void testReadObjects() throws Exception {
        KafkaConnector connector = new KafkaConnector(config);
        connector.initialize();

        KafkaConsumer<Object, Object> consumer = Mockito.mock(KafkaConsumer.class);

        Map<TopicPartition, List<ConsumerRecord<Object, Object>>> map = new HashMap<>();
        map.put(new TopicPartition("mytopic", 0), Arrays.asList(new ConsumerRecord<>("mytopic", 0, 0, "1", "hello world"),
                                                                new ConsumerRecord<>("mytopic", 0, 0, "2", "hello world!"),
                                                                new ConsumerRecord<>("mytopic", 0, 0, "3", 12345)));
        ConsumerRecords<Object, Object> buffer = new ConsumerRecords<>(map);
        ArgumentCaptor<Duration> arg = ArgumentCaptor.forClass(Duration.class);
        Mockito.doReturn(buffer).when(consumer).poll(arg.capture());

        connector.setConsumer(consumer);

        List<Object> objects = connector.read();
        Assert.assertEquals(objects.size(), 3);
        Assert.assertEquals(objects.get(0), "hello world");
        Assert.assertEquals(objects.get(1), "hello world!");
        Assert.assertEquals(objects.get(2), 12345);
        Assert.assertEquals(arg.getValue(), Duration.ofMillis(100));

        connector.close();
    }

    @Test
    public void testReadStartAtEndAndCommitSync() throws Exception {
        config.set(BulletDSLConfig.CONNECTOR_KAFKA_START_AT_END_ENABLE, true);
        config.set(BulletDSLConfig.CONNECTOR_ASYNC_COMMIT_ENABLE, false);

        KafkaConnector connector = new KafkaConnector(config);
        connector.initialize();

        KafkaConsumer<Object, Object> consumer = Mockito.mock(KafkaConsumer.class);

        Map<TopicPartition, List<ConsumerRecord<Object, Object>>> map = new HashMap<>();
        map.put(new TopicPartition("mytopic", 0), Arrays.asList(new ConsumerRecord<>("mytopic", 0, 0, "1", "hello world"),
                                                                new ConsumerRecord<>("mytopic", 0, 0, "2", "hello world!"),
                                                                new ConsumerRecord<>("mytopic", 0, 0, "3", 12345)));
        ConsumerRecords<Object, Object> buffer = new ConsumerRecords<>(map);
        ArgumentCaptor<Duration> arg = ArgumentCaptor.forClass(Duration.class);
        Mockito.doReturn(buffer).when(consumer).poll(arg.capture());

        connector.setConsumer(consumer);

        List<Object> objects = connector.read();
        Assert.assertEquals(objects.size(), 3);
        Assert.assertEquals(objects.get(0), "hello world");
        Assert.assertEquals(objects.get(1), "hello world!");
        Assert.assertEquals(objects.get(2), 12345);
        Assert.assertEquals(arg.getValue(), Duration.ofMillis(100));

        connector.close();
    }

    @Test
    public void testReadNullObjects() throws Exception {
        config.set(BulletDSLConfig.CONNECTOR_KAFKA_ENABLE_AUTO_COMMIT, true);

        KafkaConnector connector = new KafkaConnector(config);
        connector.initialize();

        KafkaConsumer<Object, Object> consumer = Mockito.mock(KafkaConsumer.class);

        Map<TopicPartition, List<ConsumerRecord<Object, Object>>> map = new HashMap<>();
        map.put(new TopicPartition("mytopic", 0), Arrays.asList(new ConsumerRecord<>("mytopic", 0, 0, "1", null),
                                                                new ConsumerRecord<>("mytopic", 0, 0, "2", null),
                                                                new ConsumerRecord<>("mytopic", 0, 0, "3", "hello world")));
        ConsumerRecords<Object, Object> buffer = new ConsumerRecords<>(map);
        ArgumentCaptor<Duration> arg = ArgumentCaptor.forClass(Duration.class);
        Mockito.doReturn(buffer).when(consumer).poll(arg.capture());

        connector.setConsumer(consumer);

        List<Object> objects = connector.read();
        Assert.assertEquals(objects.size(), 3);
        Assert.assertEquals(objects.get(2), "hello world");
        Assert.assertEquals(arg.getValue(), Duration.ofMillis(100));

        connector.close();
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Could not read from consumer\\.")
    public void testReadThrows() throws Exception {
        KafkaConnector connector = new KafkaConnector(config);
        connector.initialize();

        KafkaConsumer<Object, Object> consumer = Mockito.mock(KafkaConsumer.class);
        Mockito.doThrow(new KafkaException("mock exception")).when(consumer).poll(Mockito.any());

        connector.setConsumer(consumer);
        connector.read();
    }
}
