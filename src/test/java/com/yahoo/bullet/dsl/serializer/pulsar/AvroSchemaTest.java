package com.yahoo.bullet.dsl.serializer.pulsar;

import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.DummyAvro;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.testng.annotations.Test;


public class AvroSchemaTest {
    @Test
    public void testWithClassName() {
        BulletDSLConfig config = new BulletDSLConfig();
        config.set(AvroSchema.AVRO_CLASS_NAME, DummyAvro.class.getName());

        AvroSchema schema = new AvroSchema(config);

        DummyAvro dummyAvro = new DummyAvro();
        dummyAvro.setMyBool(true);
        dummyAvro.setMyInt(1);
        dummyAvro.setMyLong(2L);
        dummyAvro.setMyFloat(3.0f);
        dummyAvro.setMyDouble(4.0);
        dummyAvro.setMyString("5.0");

        DummyAvro another = new DummyAvro();
        another.setMyBool(false);
        another.setMyInt(2);
        another.setMyLong(3L);
        another.setMyFloat(4.0f);
        another.setMyDouble(5.0);
        another.setMyString("6.0");

        dummyAvro.setMyDummyAvro(another);

        byte[] bytes = schema.encode(dummyAvro);
        Assert.assertNotNull(bytes);

        GenericRecord message = schema.decode(bytes);

        Assert.assertEquals(message.get("myBool"), true);
        Assert.assertEquals(message.get("myInt"), 1);
        Assert.assertEquals(message.get("myLong"), 2L);
        Assert.assertEquals(message.get("myFloat"), 3.0f);
        Assert.assertEquals(message.get("myDouble"), 4.0);
        Assert.assertEquals(message.get("myString").toString(), "5.0");
        Assert.assertNotNull(message.get("myDummyAvro"));

        GenericRecord myDummyAvro = (GenericRecord) message.get("myDummyAvro");
        Assert.assertEquals(myDummyAvro.get("myBool"), false);
        Assert.assertEquals(myDummyAvro.get("myInt"), 2);
        Assert.assertEquals(myDummyAvro.get("myLong"), 3L);
        Assert.assertEquals(myDummyAvro.get("myFloat"), 4.0f);
        Assert.assertEquals(myDummyAvro.get("myDouble"), 5.0);
        Assert.assertEquals(myDummyAvro.get("myString").toString(), "6.0");
        Assert.assertNull(myDummyAvro.get("myDummyAvro"));

        // coverage
        Assert.assertNull(schema.getSchemaInfo());
    }
}
