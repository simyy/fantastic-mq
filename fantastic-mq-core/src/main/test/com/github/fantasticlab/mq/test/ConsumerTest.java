package com.github.fantasticlab.mq.test;

import com.github.fantasticlab.mq.core.Launcher;
import com.github.fantasticlab.mq.core.common.HttpClient;
import com.github.fantasticlab.mq.core.common.Message;
import com.github.fantasticlab.mq.core.consumer.Consumer;
import com.github.fantasticlab.mq.core.consumer.ConsumerClient;
import com.github.fantasticlab.mq.core.producer.Producer;
import com.github.fantasticlab.mq.core.producer.ProducerClient;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Launcher.class, webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@Slf4j
public class ConsumerTest {

    @Before
    public void setUp() throws Exception {

        Producer producer = new ProducerClient(new HttpClient(), "http://localhost:8080/producer");

        producer.send(new Message("test", "hello1"));
        producer.send(new Message("test", "hello2"));

    }

    @Test
    public void test() {

        Consumer consumer = new ConsumerClient(new HttpClient(), "http://127.0.0.1:8080");

        int offset = consumer.offset("test", "group1");
        Assert.assertTrue(offset > -1);
        Message msg = consumer.pull("test", offset);
        Assert.assertTrue(msg != null);
        boolean confirm1 = consumer.confirm("test", "group1", offset);
        Assert.assertTrue(confirm1);

        int offset2 = consumer.offset("test", "group1");
        Assert.assertTrue(offset2 > -1);
        Message msg2 = consumer.pull("test", offset2);
        Assert.assertTrue(msg2 != null);
        boolean confirm2 = consumer.confirm("test", "group1", offset);
        Assert.assertTrue(confirm2);

    }
}
