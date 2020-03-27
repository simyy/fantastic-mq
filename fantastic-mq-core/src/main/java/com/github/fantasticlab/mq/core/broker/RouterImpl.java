package com.github.fantasticlab.mq.core.broker;

public class RouterImpl implements Router {

    private final int DEFAULT = 0;

    @Override
    public int groupBy(String key, int n) {
        if (key == null) {
            return DEFAULT;
        }
        return key.hashCode() % n;
    }
}
