package com.github.fantasticlab.mq.test;

import com.github.fantasticlab.mq.core.broker.FileStorage;
import com.github.fantasticlab.mq.core.broker.Persistence;
import com.github.fantasticlab.mq.core.broker.Position;
import com.github.fantasticlab.mq.core.common.Message;
import org.junit.*;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;

@SpringBootTest
public class BrokerFileStorageTest {

    private Persistence persistence;
    private String topic = "topic1";
    private int queue = 2;

    private Position p1;
    private Position p2;

    @Before
    public void setUp() throws Exception {
        persistence = new FileStorage(topic, queue, "target/");
    }

    @After
    public void tearDown() throws Exception {
        persistence.close();
        File data = new File("target/topic1-2-db.data");
        File index = new File("target/topic1-2-db.index");
        File offset = new File("target/topic1-2-db.offset");
        data.delete();
        index.delete();
        offset.delete();
    }

    @Test
    public void testWriteAndLoad() {

        Message msg1 = new Message();
        msg1.setTopic(topic);
        msg1.setBody("body1");

        p1 = persistence.writeMsg2Disk(msg1);

        Assert.assertTrue(p1 != null);
        Assert.assertTrue(p1.getOffset() == 0l);
        Assert.assertTrue(p1.getLength() == 44l);


        Message msg2 = new Message();
        msg2.setTopic(topic);
        msg2.setBody("body1+2");

        p2 = persistence.writeMsg2Disk(msg2);

        Assert.assertTrue(p2 != null);
        Assert.assertTrue(p2.getLength() == 46l);
        Assert.assertTrue(p2.getOffset() == p1.getOffset() + p1.getLength());

        Message loadMsg1 = persistence.loadMsg(p1);
        Assert.assertTrue(loadMsg1.getBody().equals("body1"));
        Assert.assertTrue(loadMsg1.getTopic().equals(topic));

        Message loadMsg2 = persistence.loadMsg(p2);
        Assert.assertTrue(loadMsg2.getBody().equals("body1+2"));
        Assert.assertTrue(loadMsg2.getTopic().equals(topic));

    }

}
