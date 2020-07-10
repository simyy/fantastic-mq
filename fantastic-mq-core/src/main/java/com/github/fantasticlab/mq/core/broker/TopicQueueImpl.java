package com.github.fantasticlab.mq.core.broker;

import com.github.fantasticlab.mq.core.common.Message;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class TopicQueueImpl implements TopicQueue {

    public TopicQueueImpl(Storage storage) {
        this.storage = storage;
        this.groupMap = new HashMap<>();
    }

    private Map<String, TopicGroup> groupMap;

    private Storage storage;


    @Override
    public boolean push(Message msg) {
        log.info("TopicQueue PUSH MSG={}", msg);
        return storage.write(msg);
    }

    @Override
    public Message pop(int offset) {
        Message msg = storage.load(offset);
        log.info("TopicQueue POP OFFSET={} MSG={}", offset, msg);
        return msg;
    }

    @Override
    public boolean confirm(String group, int offset) {
        if (!groupMap.containsKey(group)) {
            throw new BrokerException("UNKNOWN group=" + group);
        }
        log.info("TopicQueue CONFIRM OFFSET={}", offset);
        return groupMap.get(group).confirm(offset);
    }

    @Override
    public boolean refresh(String group, int offset) {
        if (!groupMap.containsKey(group)) {
            groupMap.put(group, new TopicGroupImpl(group, offset));
        }
        return groupMap.get(group).refreshOffset(offset);
    }

    @Override
    public int offset(String group) {
        if (!groupMap.containsKey(group)) {
            groupMap.put(group, new TopicGroupImpl(group, 0));
        }
        return groupMap.get(group).getOffset(storage.getMaxOffset());
    }
}
