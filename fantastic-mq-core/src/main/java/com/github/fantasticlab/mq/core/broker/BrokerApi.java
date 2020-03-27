package com.github.fantasticlab.mq.core.broker;

import com.github.fantasticlab.mq.core.common.ApiResult;
import com.github.fantasticlab.mq.core.common.Message;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.Serializable;

@Slf4j
@RestController
public class BrokerApi {

    @Autowired
    private BrokerServer brokerServer;

    @PostMapping("/producer")
    public ApiResult<Void> producer(@RequestBody Message msg) {
        log.info("/producer\tmsg={}", msg);
        boolean rs = brokerServer.push(msg);
        return new ApiResult(rs);
    }

    @GetMapping("/consumer")
    public ApiResult<Message> consumer(@RequestParam(required = false) String key,
                                       @RequestParam String topic,
                                       @RequestParam int offset) {
        log.info("/consumer\tkey={}\ttopic={}\toffset={}", key, topic, offset);
        Message msg = brokerServer.pop(key, topic, offset);
        return new ApiResult(msg);
    }

    @GetMapping("/offset")
    public ApiResult<Integer> offset(@RequestParam(required = false) String key,
                                     @RequestParam String topic,
                                     @RequestParam String group) {
        log.info("/offset\tkey={}\ttopic={}\tgroup={}", key, topic, group);
        int offset = brokerServer.offset(key, topic, group);
        return new ApiResult(offset);
    }

    @PostMapping("/confirm")
    public ApiResult<Void> confirm(@RequestBody ConfirmR confirmR) {
        log.info("/confirm\tkey={}\ttopic={}\tgroup={}\toffset={}", confirmR.getKey(),
                confirmR.getTopic(), confirmR.getGroup(), confirmR.getOffset());
        boolean rs = brokerServer.confirm(confirmR.getKey(),
                confirmR.getTopic(), confirmR.getGroup(), confirmR.getOffset());
        return new ApiResult(rs);
    }

    @PostMapping("/refresh")
    public ApiResult<Void> refresh(@RequestBody RefreshR refreshR) {
        log.info("/refresh\tkey={}\ttopic={}\tgroup={}\toffset={}", refreshR.getKey(),
                refreshR.getTopic(), refreshR.getGroup(), refreshR.getOffset());
        boolean rs = brokerServer.refresh(refreshR.getKey(),
                refreshR.getTopic(), refreshR.getGroup(), refreshR.getOffset());
        return new ApiResult(rs);
    }

    @Data
    static class RefreshR implements Serializable {
        private String key;
        private String topic;
        private String group;
        private int offset;
    }

    @Data
    static class ConfirmR implements Serializable {
        private String key;
        private String topic;
        private String group;
        private int offset;
    }

}
