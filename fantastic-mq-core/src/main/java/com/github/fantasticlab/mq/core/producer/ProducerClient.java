package com.github.fantasticlab.mq.core.producer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.fantasticlab.mq.core.common.*;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ProducerClient implements Producer {

    public ProducerClient(Http http, String url) {
        this.http = http;
        this.url = url;
    }

    private Http http;

    private String url;

    @Override
    public boolean send(Message msg) {
        try {
            Map map = new HashMap();
            map.put("topic", msg.getTopic());
            map.put("body", msg.getBody());
            ApiResult<Void> rs = http.doPost(url, map, new TypeReference<ApiResult<Void>>(){});
            return rs.isOK();
        } catch (HttpException e) {
            log.error("ProducerClient Push error", e);
            return false;
        }

    }

    public static void main(String[] args) {

        Producer producer = new ProducerClient(new HttpClient(), "http://localhost:8080/producer");

        boolean rs1 = producer.send(new Message("test", "hello1"));
        boolean rs2 = producer.send(new Message("test", "hello2"));
        boolean rs3 = producer.send(new Message("test", "hello3"));

        assert rs1;
        assert rs2;
        assert rs3;

    }
}
