package com.github.fantasticlab.mq.core.broker;

import com.github.fantasticlab.mq.core.common.ApiResult;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class BrokerHandler {

    @ExceptionHandler(value = BrokerException.class)
    public ApiResult exceptionHandler(BrokerException e){
        return new ApiResult(false);
    }

}
