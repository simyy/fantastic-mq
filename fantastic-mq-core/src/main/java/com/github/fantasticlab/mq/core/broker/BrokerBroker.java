package com.github.fantasticlab.mq.core.broker;

import com.github.fantasticlab.mq.core.common.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class BrokerBroker implements Broker {

    /* 主题及其队列(包括分组/偏移量) */
    private List<String> topics = new ArrayList<>();
    private Map<String, TopicQueue> topicQueueMap = new HashMap<>();


    @Override
    public boolean push(Message msg) {
        if (msg == null || msg.getTopic() == null || msg.getBody() == null) {
            throw new BrokerException("The received msg or topic or body is null");
        }
        String topic = msg.getTopic();
        if (!topics.contains(topic)) {
            log.info("Broker New Topic [{}]", topic);
            topics.add(topic);
            topicQueueMap.put(topic, new TopicQueueImpl(new MemoryStorage()));
        }
        TopicQueue topicQueue = topicQueueMap.get(topic);
        return topicQueue.push(msg);
    }

    @Override
    public Message pop(String topic, int offset) {
        if (topic == null || offset < 0) {
            throw new BrokerException("Topic is null or Offset < 0");
        }
        if (!topics.contains(topic)) {
            log.info("Broker POP NULL [No ProducerClient Pushed]");
            return null;
        }
        TopicQueue topicQueue = topicQueueMap.get(topic);
        return topicQueue.pop(offset);
    }

    @Override
    public boolean confirm(String topic, String group, int offset) {
        if (topic == null || group == null || offset < 0) {
            throw new BrokerException("Topic is null or Group is null or Offset < 0");
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
            throw new BrokerException("Topic is null or Group is null or Offset < 0");
        }
        if (!topics.contains(topic)) {
            log.info("Broker POP NULL [No ProducerClient Pushed]");
            return false;
        }
        TopicQueue topicQueue = topicQueueMap.get(topic);
        return topicQueue.refresh(group, offset);
    }
    
}
