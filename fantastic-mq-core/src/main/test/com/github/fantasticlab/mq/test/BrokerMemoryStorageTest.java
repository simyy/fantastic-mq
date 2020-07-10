package com.github.fantasticlab.mq.test;

import com.github.fantasticlab.mq.core.broker.MemoryStorage;
import com.github.fantasticlab.mq.core.broker.Storage;
import com.github.fantasticlab.mq.core.common.Message;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class BrokerMemoryStorageTest {

    private String topic = "topic1";
    private int queue;
    private Storage storage;

    @Before
    public void setUp() throws Exception {
        storage = new MemoryStorage(topic, queue);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testWriteAndLoad() {

        Message msg1 = new Message();
        msg1.setTopic(topic);
        msg1.setBody("body1");

        boolean rs1 = storage.write(msg1);
        Assert.assertTrue(rs1);

        Message msg2 = new Message();
        msg2.setTopic(topic);
        msg2.setBody("body1+2");

        boolean rs2 = storage.write(msg2);
        Assert.assertTrue(rs2);

        Message loadMsg1 = storage.load(0);
        Assert.assertTrue(loadMsg1.getBody().equals("body1"));
        Assert.assertTrue(loadMsg1.getTopic().equals(topic));

        Message loadMsg2 = storage.load(1);
        Assert.assertTrue(loadMsg2.getBody().equals("body1+2"));
        Assert.assertTrue(loadMsg2.getTopic().equals(topic));



    }
}
