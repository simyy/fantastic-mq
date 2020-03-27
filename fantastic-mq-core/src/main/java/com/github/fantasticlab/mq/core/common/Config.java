package com.github.fantasticlab.mq.core.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Config implements Serializable {

    private Map<String, Topic> topicMap;

    public Config() {
        topicMap = new HashMap<>();
    }

    public void add(String topic, int queues) {
        if (!topicMap.containsKey(topic)) {
            topicMap.put(topic, new Topic(topic, queues));
        }
    }

    public Topic get(String topic) {
        return topicMap.get(topic);
    }

    @Getter
    @AllArgsConstructor
    public static class Topic {
        private String name;
        private int queues;
    }

}
