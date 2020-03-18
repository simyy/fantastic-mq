package com.github.fantasticlab.mq.core.broker;

public interface TopicGroup {

    boolean add(String consumer);

    boolean remove(String consumer);

    boolean refreshOffset(int offset);

    int getOffset(int maxOffset);

    boolean confirm(int offset);

}
