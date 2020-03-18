package com.github.fantasticlab.mq.core.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.io.Serializable;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ApiResult<T extends Object> implements Serializable {
    private int code = 0;
    private T msg;

    public ApiResult() {
    }

    public ApiResult(boolean isOK) {
        if (!isOK) {
            this.code = CODE.UNKNOWN;
        }
    }
    public ApiResult(T msg) {
        this.msg = msg;
    }
    public boolean isOK() {
        return code == 0;
    }

    static class CODE {
        public static final int UNKNOWN = 1;
    }
}
