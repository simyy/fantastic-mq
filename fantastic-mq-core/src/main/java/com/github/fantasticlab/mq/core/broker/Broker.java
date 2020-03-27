package com.github.fantasticlab.mq.core.broker;

import com.github.fantasticlab.mq.core.common.Message;

public interface Broker {

    boolean push(Message msg);

    Message pop(String key, String topic, int offset);

    int offset(String key, String topic, String group);

    boolean confirm(String key, String topic, String group, int offset);

    boolean refresh(String key, String topic, String group, int offset);

}
