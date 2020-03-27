package com.github.fantasticlab.mq.core.common;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Config implements Serializable {

    private List<TopicConfig> topicConfigs = new ArrayList<>();

    public void add(TopicConfig topicConfig) {
        topicConfigs.add(topicConfig);
    }

    public TopicConfig getTopic(String topic) {
        for (TopicConfig topicConfig : topicConfigs) {
            if (topicConfig.getName().equals(topic)) {
                return topicConfig;
            }
        }
        return null;
    }

    @Data
    public static class TopicConfig {
        private String name;
        private QueueConfig queueConfig;
    }

    @Data
    public static class QueueConfig {
        private int quantity = 1;
    }

}
