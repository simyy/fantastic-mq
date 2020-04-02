package com.github.fantasticlab.mq.core.broker;

import lombok.extern.slf4j.Slf4j;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class TopicGroupImpl implements TopicGroup {

    public TopicGroupImpl(String group, int offset) {
        this.group = group;
        this.offset = offset;
    }

    private String group;

    private int offset = 0;

    private List<String> consumers;

    private final Long OFFSET_EXPIRE = 10000l; // 10s

    // 死信队列（并发）
    private Map<Integer, Long> offsetExpireMap = new ConcurrentHashMap<>();

    private boolean isExpired(Long timestamp) {
        Date now = new Date();
        if (now.getTime() > timestamp) {
            return true;
        }
        return false;
    }

    private Long getNextExpire() {
        return new Date().getTime() + OFFSET_EXPIRE;
    }

    @Override
    public boolean add(String consumer) {
        consumers.add(consumer);
        return true;
    }

    @Override
    public boolean remove(String consumer) {
        consumers.remove(consumer);
        return true;
    }

    @Override
    synchronized public boolean refreshOffset(int offset) {
        this.offset = offset;
        offsetExpireMap.clear();
        return true;
    }

    @Override
    synchronized public int getOffset(int maxOffset) {
        if (offsetExpireMap != null && offsetExpireMap.size() > 0) {
            for (Integer key : offsetExpireMap.keySet()) {
                if (isExpired(offsetExpireMap.get(key))) {
                    offsetExpireMap.put(key, getNextExpire());
                    return key;
                }
            }
        }
        if (offset > maxOffset) {
            return -1;
        }
        offsetExpireMap.put(offset, getNextExpire());
        return offset++;
    }

    @Override
    public boolean confirm(int offset) {
        if (!offsetExpireMap.containsKey(offset)) {
            log.info("Broker duplicate consumer group={} offset={}", group, offset);
            return true;
        }
        offsetExpireMap.remove(offset);
        return true;
    }

}
