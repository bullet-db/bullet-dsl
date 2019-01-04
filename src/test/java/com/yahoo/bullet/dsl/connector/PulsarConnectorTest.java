/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.connector;

import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.dsl.serializer.pulsar.PulsarSchema;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.shade.io.netty.buffer.Unpooled;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PulsarConnectorTest {

    public static class MockAuthentication implements Authentication {

        public MockAuthentication() {
        }

        @Override
        public String getAuthMethodName() {
            return null;
        }

        @Override
        public AuthenticationDataProvider getAuthData() throws PulsarClientException {
            return null;
        }

        @Override
        public void configure(Map<String, String> map) {
        }

        @Override
        public void start() throws PulsarClientException {
        }

        @Override
        public void close() throws IOException {
        }
    }

    private BulletDSLConfig config;

    @BeforeMethod
    public void init() {
        config = new BulletDSLConfig("test_connector_config.yaml");
    }

    @Test
    public void testReadObjects() throws Exception {
        PulsarConnector connector = new PulsarConnector(config);

        ConsumerBuilder<Serializable> consumerBuilder = Mockito.mock(ConsumerBuilder.class);
        Consumer<Serializable> consumer = Mockito.mock(Consumer.class);

        Schema<Serializable> schema = new PulsarSchema();

        Mockito.doReturn(consumer).when(consumerBuilder).subscribe();
        Mockito.when(consumer.receive(Mockito.eq(100), Mockito.any()))
               .thenReturn(new MessageImpl<>("mytopic", "1:1", Collections.emptyMap(), Unpooled.wrappedBuffer(schema.encode("hello world")), schema))
               .thenReturn(new MessageImpl<>("mytopic", "2:2", Collections.emptyMap(), Unpooled.wrappedBuffer(schema.encode(null)), schema))
               .thenReturn(new MessageImpl<>("mytopic", "3:3", Collections.emptyMap(), Unpooled.wrappedBuffer(schema.encode("hello world!")), schema))
               .thenReturn(new MessageImpl<>("mytopic", "3:3", Collections.emptyMap(), Unpooled.wrappedBuffer(schema.encode(12345)), schema))
               .thenReturn(null);

        connector.setConsumerBuilder((ConsumerBuilder) consumerBuilder);
        connector.initialize();

        List<Object> objects = connector.read();
        Assert.assertEquals(objects.size(), 4);
        Assert.assertEquals(objects.get(0), "hello world");
        Assert.assertEquals(objects.get(1), null);
        Assert.assertEquals(objects.get(2), "hello world!");
        Assert.assertEquals(objects.get(3), 12345);

        connector.close();
    }

    @Test
    public void testReadAckSync() throws Exception {
        config.set(BulletDSLConfig.CONNECTOR_ASYNC_COMMIT_ENABLE, false);

        PulsarConnector connector = new PulsarConnector(config);

        ConsumerBuilder<Serializable> consumerBuilder = Mockito.mock(ConsumerBuilder.class);
        Consumer<Serializable> consumer = Mockito.mock(Consumer.class);

        Schema<Serializable> schema = new PulsarSchema();

        Mockito.doReturn(consumer).when(consumerBuilder).subscribe();
        Mockito.when(consumer.receive(Mockito.eq(100), Mockito.any()))
               .thenReturn(new MessageImpl<>("mytopic", "1:1", Collections.emptyMap(), Unpooled.wrappedBuffer(schema.encode("hello world")), schema))
               .thenReturn(new MessageImpl<>("mytopic", "2:2", Collections.emptyMap(), Unpooled.wrappedBuffer(schema.encode(null)), schema))
               .thenReturn(new MessageImpl<>("mytopic", "3:3", Collections.emptyMap(), Unpooled.wrappedBuffer(schema.encode("hello world!")), schema))
               .thenReturn(new MessageImpl<>("mytopic", "3:3", Collections.emptyMap(), Unpooled.wrappedBuffer(schema.encode(12345)), schema))
               .thenReturn(null);

        connector.setConsumerBuilder((ConsumerBuilder) consumerBuilder);
        connector.initialize();

        List<Object> objects = connector.read();
        Assert.assertEquals(objects.size(), 4);
        Assert.assertEquals(objects.get(0), "hello world");
        Assert.assertEquals(objects.get(1), null);
        Assert.assertEquals(objects.get(2), "hello world!");
        Assert.assertEquals(objects.get(3), 12345);

        connector.close();
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Could not acknowledge message\\.")
    public void testReadAckSyncThrows() throws Exception {
        config.set(BulletDSLConfig.CONNECTOR_ASYNC_COMMIT_ENABLE, false);

        PulsarConnector connector = new PulsarConnector(config);

        Consumer<Serializable> consumer = Mockito.mock(Consumer.class);
        Schema<Serializable> schema = new PulsarSchema();
        Mockito.when(consumer.receive(Mockito.eq(100), Mockito.any()))
                .thenReturn(new MessageImpl<>("mytopic", "1:1", Collections.emptyMap(), Unpooled.wrappedBuffer(schema.encode("hello world")), schema));
        Mockito.doThrow(new PulsarClientException("mock exception")).when(consumer).acknowledge(Mockito.any(Message.class));

        // skip initialize
        connector.setConsumer((Consumer) consumer);
        connector.read();
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Could not create consumer\\.")
    public void testReadConsumerBuilderThrows() throws Exception {
        PulsarConnector connector = new PulsarConnector(config);

        ConsumerBuilder<Object> consumerBuilder = Mockito.mock(ConsumerBuilder.class);
        Mockito.doThrow(new PulsarClientException("mock exception")).when(consumerBuilder).subscribe();

        connector.setConsumerBuilder(consumerBuilder);
        connector.initialize();
        connector.read();
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Could not read from consumer\\.")
    public void testReadConsumerThrows() throws Exception {
        PulsarConnector connector = new PulsarConnector(config);

        Consumer<Object> consumer = Mockito.mock(Consumer.class);
        Mockito.doThrow(new PulsarClientException("mock exception")).when(consumer).receive(Mockito.anyInt(), Mockito.any());

        // skip initialize
        connector.setConsumer(consumer);
        connector.read();
    }

    @Test
    public void testAuthenticationConfigs() throws Exception {
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_AUTH_ENABLE, true);
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_AUTH_PLUGIN_CLASS_NAME, "com.yahoo.bullet.dsl.connector.PulsarConnectorTest$MockAuthentication");
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_AUTH_PARAMS_STRING, "");

        PulsarConnector connector = new PulsarConnector(config);

        Assert.assertTrue(connector.getClient() instanceof PulsarClientImpl);
        Assert.assertTrue(((PulsarClientImpl) connector.getClient()).getConfiguration().getAuthentication() instanceof MockAuthentication);
    }
}
