package com.github.fantasticlab.mq.core.broker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fantasticlab.mq.core.common.Message;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class MemoryStorage implements Storage {

    private List<String> array = new CopyOnWriteArrayList<>();

    private String topic;

    private int queue;

    public MemoryStorage(String topic, int queue) {
        this.topic = topic;
        this.queue = queue;
    }

    @Override
    public int getMaxOffset() {
        return array.size() - 1;
    }

    @Override
    public boolean add(Message msg) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            String jsonVal = mapper.writeValueAsString(msg);
            array.add(jsonVal);
        } catch (JsonProcessingException e) {
            throw new StorageException("JsonSerialize error");
        }
        return true;
    }

    @Override
    public Message get(int offset) {
        if (offset < 0 || offset > array.size()) {
            throw new StorageException("Position < 0 or Position > max length");
        }
        String jsonVal = array.get(offset);
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(jsonVal, Message.class);
        } catch (IOException e) {
            throw new StorageException("JsonDeserialize error");
        }
    }
}
