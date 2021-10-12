/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.connector;

import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.dsl.DummyAvro;
import com.yahoo.bullet.dsl.DummyOuterClass;
import com.yahoo.bullet.dsl.serializer.pulsar.JavaSchema;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.ProtobufSchema;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.shade.io.netty.buffer.Unpooled;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
        public AuthenticationDataProvider getAuthData() {
            return null;
        }

        @Override
        public void configure(Map<String, String> map) {
        }

        @Override
        public void start() {
        }

        @Override
        public void close() {
        }
    }

    private BulletDSLConfig config;

    @BeforeMethod
    public void init() {
        config = new BulletDSLConfig("test_connector_config.yaml");
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Could not create Pulsar consumer\\.")
    public void testInitializeConsumerFails() throws Exception {
        PulsarConnector connector = new PulsarConnector(config);
        connector.initialize();
    }

    @Test
    public void testGetSchemaBytes() throws Exception {
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_SCHEMA_TYPE, BulletDSLConfig.PULSAR_SCHEMA_BYTES);
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_SCHEMA_CLASS_NAME, null);

        PulsarConnector connector = new PulsarConnector(config);
        PulsarConnector connectorSpy = Mockito.spy(connector);

        PulsarClient client = Mockito.mock(PulsarClient.class);
        ConsumerBuilder consumerBuilder = Mockito.mock(ConsumerBuilder.class);
        Consumer consumer = Mockito.mock(Consumer.class);
        ArgumentCaptor<Schema> schema = ArgumentCaptor.forClass(Schema.class);

        Mockito.doReturn(client).when(connectorSpy).getPulsarClient(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.doReturn(consumerBuilder).when(client).newConsumer(schema.capture());
        Mockito.doReturn(consumerBuilder).when(consumerBuilder).loadConf(Mockito.any());
        Mockito.doReturn(consumerBuilder).when(consumerBuilder).topics(Mockito.any());
        Mockito.doReturn(consumer).when(consumerBuilder).subscribe();

        connectorSpy.initialize();
        connectorSpy.close();

        Assert.assertTrue(schema.getValue() instanceof BytesSchema);
    }

    @Test
    public void testGetSchemaString() throws Exception {
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_SCHEMA_TYPE, BulletDSLConfig.PULSAR_SCHEMA_STRING);
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_SCHEMA_CLASS_NAME, null);

        PulsarConnector connector = new PulsarConnector(config);
        PulsarConnector connectorSpy = Mockito.spy(connector);

        PulsarClient client = Mockito.mock(PulsarClient.class);
        ConsumerBuilder consumerBuilder = Mockito.mock(ConsumerBuilder.class);
        Consumer consumer = Mockito.mock(Consumer.class);
        ArgumentCaptor<Schema> schema = ArgumentCaptor.forClass(Schema.class);

        Mockito.doReturn(client).when(connectorSpy).getPulsarClient(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.doReturn(consumerBuilder).when(client).newConsumer(schema.capture());
        Mockito.doReturn(consumerBuilder).when(consumerBuilder).loadConf(Mockito.any());
        Mockito.doReturn(consumerBuilder).when(consumerBuilder).topics(Mockito.any());
        Mockito.doReturn(consumer).when(consumerBuilder).subscribe();

        connectorSpy.initialize();
        connectorSpy.close();

        Assert.assertTrue(schema.getValue() instanceof StringSchema);
    }

    @Test
    public void testGetSchemaJson() throws Exception {
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_SCHEMA_TYPE, BulletDSLConfig.PULSAR_SCHEMA_JSON);
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_SCHEMA_CLASS_NAME, DummyAvro.class.getName());

        PulsarConnector connector = new PulsarConnector(config);
        PulsarConnector connectorSpy = Mockito.spy(connector);

        PulsarClient client = Mockito.mock(PulsarClient.class);
        ConsumerBuilder consumerBuilder = Mockito.mock(ConsumerBuilder.class);
        Consumer consumer = Mockito.mock(Consumer.class);
        ArgumentCaptor<Schema> schema = ArgumentCaptor.forClass(Schema.class);

        Mockito.doReturn(client).when(connectorSpy).getPulsarClient(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.doReturn(consumerBuilder).when(client).newConsumer(schema.capture());
        Mockito.doReturn(consumerBuilder).when(consumerBuilder).loadConf(Mockito.any());
        Mockito.doReturn(consumerBuilder).when(consumerBuilder).topics(Mockito.any());
        Mockito.doReturn(consumer).when(consumerBuilder).subscribe();

        connectorSpy.initialize();
        connectorSpy.close();

        Assert.assertTrue(schema.getValue() instanceof JSONSchema);
    }

    @Test
    public void testGetSchemaAvro() throws Exception {
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_SCHEMA_TYPE, BulletDSLConfig.PULSAR_SCHEMA_AVRO);
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_SCHEMA_CLASS_NAME, DummyAvro.class.getName());

        PulsarConnector connector = new PulsarConnector(config);
        PulsarConnector connectorSpy = Mockito.spy(connector);

        PulsarClient client = Mockito.mock(PulsarClient.class);
        ConsumerBuilder consumerBuilder = Mockito.mock(ConsumerBuilder.class);
        Consumer consumer = Mockito.mock(Consumer.class);
        ArgumentCaptor<Schema> schema = ArgumentCaptor.forClass(Schema.class);

        Mockito.doReturn(client).when(connectorSpy).getPulsarClient(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.doReturn(consumerBuilder).when(client).newConsumer(schema.capture());
        Mockito.doReturn(consumerBuilder).when(consumerBuilder).loadConf(Mockito.any());
        Mockito.doReturn(consumerBuilder).when(consumerBuilder).topics(Mockito.any());
        Mockito.doReturn(consumer).when(consumerBuilder).subscribe();

        connectorSpy.initialize();
        connectorSpy.close();

        Assert.assertTrue(schema.getValue() instanceof AvroSchema);
    }

    @Test
    public void testGetSchemaProtobuf() throws Exception {
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_SCHEMA_TYPE, BulletDSLConfig.PULSAR_SCHEMA_PROTOBUF);
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_SCHEMA_CLASS_NAME, DummyOuterClass.Dummy.class.getName());

        PulsarConnector connector = new PulsarConnector(config);
        PulsarConnector connectorSpy = Mockito.spy(connector);

        PulsarClient client = Mockito.mock(PulsarClient.class);
        ConsumerBuilder consumerBuilder = Mockito.mock(ConsumerBuilder.class);
        Consumer consumer = Mockito.mock(Consumer.class);
        ArgumentCaptor<Schema> schema = ArgumentCaptor.forClass(Schema.class);

        Mockito.doReturn(client).when(connectorSpy).getPulsarClient(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.doReturn(consumerBuilder).when(client).newConsumer(schema.capture());
        Mockito.doReturn(consumerBuilder).when(consumerBuilder).loadConf(Mockito.any());
        Mockito.doReturn(consumerBuilder).when(consumerBuilder).topics(Mockito.any());
        Mockito.doReturn(consumer).when(consumerBuilder).subscribe();

        connectorSpy.initialize();
        connectorSpy.close();

        Assert.assertTrue(schema.getValue() instanceof ProtobufSchema);
    }

    @Test
    public void testGetSchemaCustom() throws Exception {
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_SCHEMA_TYPE, BulletDSLConfig.PULSAR_SCHEMA_CUSTOM);
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_SCHEMA_CLASS_NAME, JavaSchema.class.getName());

        PulsarConnector connector = new PulsarConnector(config);
        PulsarConnector connectorSpy = Mockito.spy(connector);

        PulsarClient client = Mockito.mock(PulsarClient.class);
        ConsumerBuilder consumerBuilder = Mockito.mock(ConsumerBuilder.class);
        Consumer consumer = Mockito.mock(Consumer.class);
        ArgumentCaptor<Schema> schema = ArgumentCaptor.forClass(Schema.class);

        Mockito.doReturn(client).when(connectorSpy).getPulsarClient(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.doReturn(consumerBuilder).when(client).newConsumer(schema.capture());
        Mockito.doReturn(consumerBuilder).when(consumerBuilder).loadConf(Mockito.any());
        Mockito.doReturn(consumerBuilder).when(consumerBuilder).topics(Mockito.any());
        Mockito.doReturn(consumer).when(consumerBuilder).subscribe();

        connectorSpy.initialize();
        connectorSpy.close();

        Assert.assertTrue(schema.getValue() instanceof JavaSchema);
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Could not create Pulsar schema\\.")
    public void testGetSchemaFails() throws Exception {
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_SCHEMA_TYPE, BulletDSLConfig.PULSAR_SCHEMA_CUSTOM);
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_SCHEMA_CLASS_NAME, DummyAvro.class.getName());

        PulsarConnector connector = new PulsarConnector(config);
        PulsarConnector connectorSpy = Mockito.spy(connector);

        PulsarClient client = Mockito.mock(PulsarClient.class);
        ConsumerBuilder consumerBuilder = Mockito.mock(ConsumerBuilder.class);
        Consumer consumer = Mockito.mock(Consumer.class);
        ArgumentCaptor<Schema> schema = ArgumentCaptor.forClass(Schema.class);

        Mockito.doReturn(client).when(connectorSpy).getPulsarClient(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.doReturn(consumerBuilder).when(client).newConsumer(schema.capture());
        Mockito.doReturn(consumerBuilder).when(consumerBuilder).loadConf(Mockito.any());
        Mockito.doReturn(consumerBuilder).when(consumerBuilder).topics(Mockito.any());
        Mockito.doReturn(consumer).when(consumerBuilder).subscribe();

        connectorSpy.initialize();
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Pulsar schema type must be one of: .*")
    public void testGetSchemaBadType() throws Exception {
        PulsarConnector connector = new PulsarConnector(config);
        PulsarConnector connectorSpy = Mockito.spy(connector);

        connector.getConfig().set(BulletDSLConfig.CONNECTOR_PULSAR_SCHEMA_TYPE, "BADTYPE");

        PulsarClient client = Mockito.mock(PulsarClient.class);
        ConsumerBuilder consumerBuilder = Mockito.mock(ConsumerBuilder.class);
        Consumer consumer = Mockito.mock(Consumer.class);
        ArgumentCaptor<Schema> schema = ArgumentCaptor.forClass(Schema.class);

        Mockito.doReturn(client).when(connectorSpy).getPulsarClient(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.doReturn(consumerBuilder).when(client).newConsumer(schema.capture());
        Mockito.doReturn(consumerBuilder).when(consumerBuilder).loadConf(Mockito.any());
        Mockito.doReturn(consumerBuilder).when(consumerBuilder).topics(Mockito.any());
        Mockito.doReturn(consumer).when(consumerBuilder).subscribe();

        connectorSpy.initialize();
    }

    @Test
    public void testGetPulsarClient() {
        PulsarConnector connector = new PulsarConnector(config);

        try {
            connector.initialize();
        } catch (BulletDSLException ignored) {
        }

        // Initialize throws because the consumer can't be created, but at least the PulsarClient is successfully created
        Assert.assertTrue(connector.getClient() instanceof PulsarClientImpl);
        Assert.assertTrue(((PulsarClientImpl) connector.getClient()).getConfiguration().getAuthentication() instanceof AuthenticationDisabled);
    }

    @Test
    public void testGetPulsarClientAuthentication() {
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_AUTH_ENABLE, true);
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_AUTH_PLUGIN_CLASS_NAME, MockAuthentication.class.getName());
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_AUTH_PARAMS_STRING, "");

        PulsarConnector connector = new PulsarConnector(config);

        try {
            connector.initialize();
        } catch (BulletDSLException ignored) {
        }

        // Initialize throws because the consumer can't be created, but at least the PulsarClient is successfully created
        Assert.assertTrue(connector.getClient() instanceof PulsarClientImpl);
        Assert.assertTrue(((PulsarClientImpl) connector.getClient()).getConfiguration().getAuthentication() instanceof MockAuthentication);
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Could not create Pulsar client\\.")
    public void testGetPulsarClientFails() throws Exception {
        config.set(BulletDSLConfig.CONNECTOR_PULSAR_CLIENT_SERVICE_URL, "");

        PulsarConnector connector = new PulsarConnector(config);
        connector.initialize();
    }

    @Test
    public void testReadObjects() throws Exception {
        PulsarConnector connector = new PulsarConnector(config);
        Consumer<Serializable> consumer = Mockito.mock(Consumer.class);
        JavaSchema schema = new JavaSchema();

        Mockito.when(consumer.receive(Mockito.eq(100), Mockito.any()))
                .thenReturn(new MessageImpl<>("mytopic", "1:1", Collections.emptyMap(), Unpooled.wrappedBuffer(schema.encode("hello world")), schema))
                .thenReturn(new MessageImpl<>("mytopic", "2:2", Collections.emptyMap(), Unpooled.wrappedBuffer(schema.encode(null)), schema))
                .thenReturn(new MessageImpl<>("mytopic", "3:3", Collections.emptyMap(), Unpooled.wrappedBuffer(schema.encode("hello world!")), schema))
                .thenReturn(new MessageImpl<>("mytopic", "3:3", Collections.emptyMap(), Unpooled.wrappedBuffer(schema.encode(12345)), schema))
                .thenReturn(null);

        // skip initialize
        connector.setConsumer((Consumer) consumer);

        List<Object> objects = connector.read();
        Assert.assertEquals(objects.size(), 4);
        Assert.assertEquals(objects.get(0), "hello world");
        Assert.assertEquals(objects.get(1), null);
        Assert.assertEquals(objects.get(2), "hello world!");
        Assert.assertEquals(objects.get(3), 12345);
    }

    @Test
    public void testReadAckSync() throws Exception {
        config.set(BulletDSLConfig.CONNECTOR_ASYNC_COMMIT_ENABLE, false);

        PulsarConnector connector = new PulsarConnector(config);
        Consumer<Serializable> consumer = Mockito.mock(Consumer.class);
        Schema<Serializable> schema = new JavaSchema();

        Mockito.when(consumer.receive(Mockito.eq(100), Mockito.any()))
               .thenReturn(new MessageImpl<>("mytopic", "1:1", Collections.emptyMap(), Unpooled.wrappedBuffer(schema.encode("hello world")), schema))
               .thenReturn(new MessageImpl<>("mytopic", "2:2", Collections.emptyMap(), Unpooled.wrappedBuffer(schema.encode(null)), schema))
               .thenReturn(new MessageImpl<>("mytopic", "3:3", Collections.emptyMap(), Unpooled.wrappedBuffer(schema.encode("hello world!")), schema))
               .thenReturn(new MessageImpl<>("mytopic", "3:3", Collections.emptyMap(), Unpooled.wrappedBuffer(schema.encode(12345)), schema))
               .thenReturn(null);

        // skip initialize
        connector.setConsumer((Consumer) consumer);

        List<Object> objects = connector.read();
        Assert.assertEquals(objects.size(), 4);
        Assert.assertEquals(objects.get(0), "hello world");
        Assert.assertEquals(objects.get(1), null);
        Assert.assertEquals(objects.get(2), "hello world!");
        Assert.assertEquals(objects.get(3), 12345);
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Could not acknowledge message\\.")
    public void testReadAckSyncThrows() throws Exception {
        config.set(BulletDSLConfig.CONNECTOR_ASYNC_COMMIT_ENABLE, false);

        PulsarConnector connector = new PulsarConnector(config);
        Consumer<Serializable> consumer = Mockito.mock(Consumer.class);
        Schema<Serializable> schema = new JavaSchema();

        Mockito.when(consumer.receive(Mockito.eq(100), Mockito.any()))
               .thenReturn(new MessageImpl<>("mytopic", "1:1", Collections.emptyMap(), Unpooled.wrappedBuffer(schema.encode("hello world")), schema));
        Mockito.doThrow(new PulsarClientException("mock exception")).when(consumer).acknowledge(Mockito.any(Message.class));

        // skip initialize
        connector.setConsumer((Consumer) consumer);
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

}
