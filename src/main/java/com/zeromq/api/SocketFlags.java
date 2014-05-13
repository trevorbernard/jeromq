package com.zeromq.api;

public enum SocketFlags {
    NONE(0), DONT_WAIT(1), SEND_MORE(2);

    private final int flag;

    SocketFlags(int flag) {
        this.flag = flag;
    }

    public int getFlag() {
        return this.flag;
    }
}
