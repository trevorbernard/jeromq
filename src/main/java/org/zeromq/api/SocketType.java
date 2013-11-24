package org.zeromq.api;

import zmq.ZMQ;

public enum SocketType {
    PAIR(ZMQ.ZMQ_PAIR), PUB(ZMQ.ZMQ_PUB), SUB(ZMQ.ZMQ_SUB), REQ(ZMQ.ZMQ_REQ), REP(ZMQ.ZMQ_REP), DEALER(ZMQ.ZMQ_DEALER), ROUTER(
            ZMQ.ZMQ_ROUTER), PULL(ZMQ.ZMQ_PULL), PUSH(ZMQ.ZMQ_PUSH), XPUB(ZMQ.ZMQ_XPUB), XSUB(ZMQ.ZMQ_XSUB), XREQ(
            ZMQ.ZMQ_XREQ), XREP(ZMQ.ZMQ_XREP);

    private final int value;

    SocketType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
