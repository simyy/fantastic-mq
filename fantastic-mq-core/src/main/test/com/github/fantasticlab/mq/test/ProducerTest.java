package com.github.fantasticlab.mq.test;

import com.github.fantasticlab.mq.core.Launcher;
import com.github.fantasticlab.mq.core.common.HttpClient;
import com.github.fantasticlab.mq.core.common.Message;
import com.github.fantasticlab.mq.core.producer.Producer;
import com.github.fantasticlab.mq.core.producer.ProducerClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Launcher.class, webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
public class ProducerTest {

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void test() {

        Producer producer = new ProducerClient(new HttpClient(), "http://localhost:8080/producer");

        boolean rs1 = producer.send(new Message("test", "hello1"));
        boolean rs2 = producer.send(new Message("test", "hello2"));
        boolean rs3 = producer.send(new Message("test", "hello3"));

        Assert.assertTrue(rs1);
        Assert.assertTrue(rs2);
        Assert.assertTrue(rs3);

    }
}
