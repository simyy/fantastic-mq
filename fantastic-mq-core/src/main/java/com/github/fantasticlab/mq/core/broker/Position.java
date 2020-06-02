package com.github.fantasticlab.mq.core.broker;

import lombok.Data;

import java.io.Serializable;

@Data
public class Position implements Serializable {
    private long offset;
    private long length;

    public Position(long offset, long length) {
        this.offset = offset;
        this.length = length;
    }
}
