package com.github.fantasticlab.mq.test;

import com.github.fantasticlab.mq.core.broker.Broker;
import com.github.fantasticlab.mq.core.broker.BrokerServer;
import com.github.fantasticlab.mq.core.common.Message;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;


@SpringBootTest
public class BrokerServerTest {

    private Broker broker;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerServer();
    }

    @Test
    public void testPushAndPop() {

        Message msg1 = new Message();
        msg1.setTopic("topic1");
        msg1.setBody("body1");

        boolean rs1 = broker.push(msg1);
        Assert.assertTrue(rs1);

        Message msg2 = new Message();
        msg2.setTopic("topic2");
        msg2.setBody("body2");

        boolean rs2 = broker.push(msg2);
        Assert.assertTrue(rs2);

        int offset1 = broker.offset(null, "topic1", "group1");
        Message popMsg1 = broker.pop(null, "topic1", offset1);
        Assert.assertTrue(popMsg1 != null);
        Assert.assertTrue(popMsg1.getBody().equals("body1"));

        int offset2 = broker.offset(null, "topic2", "group1");
        Message popMsg2 = broker.pop(null, "topic2", offset2);
        Assert.assertTrue(popMsg2 != null);
        Assert.assertTrue(popMsg2.getBody().equals("body2"));

    }

    @Test
    public void testOffsetAndConfirm() {

        String topic = "topic1";

        Message msg1 = new Message();
        msg1.setTopic(topic);
        msg1.setBody("body1");
        broker.push(msg1);

        Message msg2 = new Message();
        msg2.setTopic(topic);
        msg2.setBody("body2");
        broker.push(msg2);

        int offset1 = broker.offset(null, topic, "group1");
        boolean rs1 = broker.confirm(null, topic, "group1", offset1);
        Assert.assertTrue(offset1 == 0);
        Assert.assertTrue(rs1);
        offset1 = broker.offset(null, topic, "group1");
        Assert.assertTrue(offset1 == 1);

        int offset2 = broker.offset(null, topic, "group2");
        Assert.assertTrue(offset2 == 0);

    }
}
