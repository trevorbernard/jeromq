package org.zeromq.api;

import zmq.ZMQ;

public enum SocketFlag {
    NONE(0), SEND_MORE(ZMQ.ZMQ_SNDMORE), DONT_WAIT(ZMQ.ZMQ_DONTWAIT);
    
    private final int value;
    
    SocketFlag(int value) {
        this.value = value;
    }
    
    public int getValue() {
        return value;
    }
}
