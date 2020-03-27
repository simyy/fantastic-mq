package com.github.fantasticlab.mq.core.broker;

import com.github.fantasticlab.mq.core.common.Config;
import com.github.fantasticlab.mq.core.common.Message;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RouterImpl implements Router {

    private final int DEFAULT_ROUTE_KEY = 0;

    private Config config;

    public RouterImpl(Config config) {
        this.config = config;
    }

    private int getRouteKey(String key, int n) {
        if (key == null) {
            return DEFAULT_ROUTE_KEY;
        }
        int routeKey = key.hashCode() % n;
        log.info("Router\tKey={} Key.hashCode={} N={}\tRouteKey={}",
                key, key.hashCode(), n, routeKey);
        return routeKey;
    }

    @Override
    public int getRouteKey(Message msg) {
        return getRouteKey(msg.getKey(), config.get(msg.getTopic()).getQueues());
    }

    @Override
    public int getRouteKey(String key, String topic) {
        return getRouteKey(key, config.get(topic).getQueues());
    }
}
