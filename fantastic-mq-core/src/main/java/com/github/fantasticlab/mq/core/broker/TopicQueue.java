package com.github.fantasticlab.mq.core.broker;

import com.github.fantasticlab.mq.core.common.Message;

/**
 * 消息队列的实现思路：
 * 1. 追加消息存储
 * 2. 偏移量拉取数据
 * 3. 分组维护偏移量
 */
public interface TopicQueue {

    boolean push(Message msg);

    Message pop(int offset);

    boolean confirm(String group, int offset);

    boolean refresh(String group, int offset);

    int offset(String group);

}
