package com.github.fantasticlab.mq.core.consumer;

import com.github.fantasticlab.mq.core.common.Message;

public interface Consumer {

    Message pull(String topic, int offset);

    int offset(String topic, String group);

    boolean confirm(String topic, String group, int offset);
}
