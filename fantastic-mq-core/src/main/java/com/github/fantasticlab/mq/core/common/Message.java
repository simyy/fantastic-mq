package com.github.fantasticlab.mq.core.common;

import lombok.Data;

import java.io.Serializable;

@Data
public class Message implements Serializable {
    // 消息主题
    private String topic;
    // 消息体
    private String body;

    public Message() {
    }

    public Message(String topic, String body) {
        this.topic = topic;
        this.body = body;
    }

    @Override
    public String toString() {
        return "Message{" +
                "topic='" + topic + '\'' +
                ", body='" + body + '\'' +
                '}';
    }
}
