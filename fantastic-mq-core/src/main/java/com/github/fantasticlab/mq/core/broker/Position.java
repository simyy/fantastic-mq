package com.github.fantasticlab.mq.core.broker;

import lombok.Data;

import java.io.Serializable;

@Data
public class Position implements Serializable {
    private long offset;
    private long length;

}
