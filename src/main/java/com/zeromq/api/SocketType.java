package com.zeromq.api;

public enum SocketType {
    PAIR(0), PUB(1), SUB(2), REQ(3), REP(4), DEALER(5), ROUTER(6), PULL(7), PUSH(8), XPUB(9), XSUB(10);

    private int type;

    SocketType(int type) {
        this.type = type;
    }

    public int getType() {
        return this.type;
    }
}
