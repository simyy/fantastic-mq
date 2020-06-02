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

    private Config config = new Config();

    private List<String> topics = new ArrayList<>();

    // {'topic1': [TopicQueue1, TopicQueue2]}
    private Map<String, List<TopicQueue>> topicQueueMapping = new HashMap<>();

    private Router router = new RouterImpl(config);

    @Override
    public boolean push(Message msg) {
        if (msg == null || msg.getTopic() == null || msg.getBody() == null) {
            throw new BrokerException("The received msg or topic or body is null");
        }
        String topic = msg.getTopic();
        if (!topics.contains(topic)) {
            // Init A Default Topic Queue If not exist.
            log.info("Broker New Topic [{}]", topic);
            topics.add(topic);
            config.add(topic, 2);
            List<TopicQueue> queues = new ArrayList<>();
            queues.add(new TopicQueueImpl(new MemoryStorage(topic, 0)));
            queues.add(new TopicQueueImpl(new MemoryStorage(topic, 1)));
            topicQueueMapping.put(topic, queues);
        }
        return topicQueueMapping
                .get(topic)
                .get(router.getRouteKey(msg))
                .push(msg);
    }

    @Override
    public Message pop(String key, String topic, int offset) {
        if (topic == null || offset < 0) {
            throw new BrokerException("Topic is null or Offset < 0");
        }
        if (!topics.contains(topic)) {
            log.info("Broker POP NULL [No ProducerClient Pushed]");
            return null;
        }
        return topicQueueMapping
                .get(topic)
                .get(router.getRouteKey(key, topic))
                .pop(offset);
    }

    @Override
    public int offset(String key, String topic, String group) {
        if (topic == null || group == null) {
            throw new BrokerException("Topic is null or Group is null");
        }
        if (!topics.contains(topic)) {
            log.info("Broker POP NULL [No ProducerClient Pushed]");
            return 0;
        }
        return topicQueueMapping
                .get(topic)
                .get(router.getRouteKey(key, topic))
                .offset(group);
    }

    @Override
    public boolean confirm(String key, String topic, String group, int offset) {
        if (topic == null || group == null || offset < 0) {
            throw new BrokerException("Topic is null or Group is null or Offset < 0");
        }
        return topicQueueMapping
                .get(topic)
                .get(router.getRouteKey(key, topic))
                .confirm(group, offset);
    }

    @Override
    public boolean refresh(String key, String topic, String group, int offset) {
        if (topic == null || group == null || offset < 0) {
            throw new BrokerException("Topic is null or Group is null or Position < 0");
        }
        if (!topics.contains(topic)) {
            log.info("Broker POP NULL [No ProducerClient Pushed]");
            return false;
        }
        return topicQueueMapping
                .get(topic)
                .get(router.getRouteKey(key, topic))
                .refresh(group, offset);
    }
    
}
