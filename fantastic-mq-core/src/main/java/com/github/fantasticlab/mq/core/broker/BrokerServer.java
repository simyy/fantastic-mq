package com.github.fantasticlab.mq.core.broker;

import com.github.fantasticlab.mq.core.common.Message;
import com.github.fantasticlab.mq.core.common.Config;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class BrokerServer implements Broker {

    // 配置
    private static Config config = new Config();

    static {

        Config.QueueConfig queueConfig = new Config.QueueConfig();
        queueConfig.setQuantity(1);
        Config.TopicConfig topicConfig = new Config.TopicConfig();
        topicConfig.setName("test");
        topicConfig.setQueueConfig(queueConfig);
        config.add(topicConfig);
    }

    /* 主题及其队列(包括分组/偏移量) */
    private List<String> topics = new ArrayList<>();
    private Map<String, Map<Integer, TopicQueue>> topicQueueMap = new HashMap<>();

    private Router router = new RouterImpl();

    @Override
    public boolean push(Message msg) {
        if (msg == null || msg.getTopic() == null || msg.getBody() == null) {
            throw new BrokerException("The received msg or topic or body is null");
        }
        String topic = msg.getTopic();
        int routeKey = router.groupBy(msg.getKey(), config.getTopic(topic).getQueueConfig().getQuantity());
        if (!topics.contains(topic)) {
            initTopicQueue(topic, routeKey);
        }
        TopicQueue topicQueue = topicQueueMap.get(topic).get(routeKey);
        return topicQueue.push(msg);
    }

    private void initTopicQueue(String topic, Integer routeKey) {
        log.info("Broker New Topic [{}]", topic);
        topics.add(topic);
        Map<Integer, TopicQueue> queueMap = new HashMap<>();
        queueMap.put(routeKey, new TopicQueueImpl(new MemoryStorage()));
        topicQueueMap.put(topic, queueMap);
    }

    @Override
    public Message pop(String topic, int offset) {
        if (topic == null || offset < 0) {
            throw new BrokerException("Topic is null or Position < 0");
        }
        if (!topics.contains(topic)) {
            log.info("Broker POP NULL [No ProducerClient Pushed]");
            return null;
        }
        TopicQueue topicQueue = topicQueueMap.get(topic).get(0);
        return topicQueue.pop(offset);
    }

    @Override
    public boolean confirm(String topic, String group, int offset) {
        if (topic == null || group == null || offset < 0) {
            throw new BrokerException("Topic is null or Group is null or Position < 0");
        }
        return topicQueueMap.get(topic).confirm(group, offset);
    }

    @Override
    public int offset(String topic, String group) {
        if (topic == null || group == null) {
            throw new BrokerException("Topic is null or Group is null");
        }
        if (!topics.contains(topic)) {
            log.info("Broker POP NULL [No ProducerClient Pushed]");
            return 0;
        }
        TopicQueue topicQueue = topicQueueMap.get(topic);
        return topicQueue.offset(group);
    }

    @Override
    public boolean refresh(String topic, String group, int offset) {
        if (topic == null || group == null || offset < 0) {
            throw new BrokerException("Topic is null or Group is null or Position < 0");
        }
        if (!topics.contains(topic)) {
            log.info("Broker POP NULL [No ProducerClient Pushed]");
            return false;
        }
        TopicQueue topicQueue = topicQueueMap.get(topic);
        return topicQueue.refresh(group, offset);
    }
    
}
