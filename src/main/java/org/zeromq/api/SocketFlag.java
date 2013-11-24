package org.zeromq.api;

public enum SocketFlag {
    NONE(0), SEND_MORE(1), DONT_WAIT(2);
    
    private final int value;
    
    SocketFlag(int value) {
        this.value = value;
    }
    
    public int getValue() {
        return value;
    }
}
