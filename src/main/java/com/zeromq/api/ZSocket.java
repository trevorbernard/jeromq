package com.zeromq.api;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import org.zeromq.ZMQException;

import zmq.Ctx;
import zmq.Msg;
import zmq.SocketBase;

public class ZSocket implements Closeable {
    // Global Context
    private static final AtomicReference<State> CONTEXT_STATE = new AtomicReference<State>(State.LATENT);
    private static Ctx ctx;

    private enum State {
        LATENT, STARTED, STOPPED;
    }

    private final AtomicReference<State> state = new AtomicReference<ZSocket.State>(State.LATENT);

    private final SocketType type;
    private final SocketBase socket;

    public ZSocket(SocketType type) {
        if (CONTEXT_STATE.compareAndSet(State.LATENT, State.STARTED)) {
            this.ctx = zmq.ZMQ.zmq_init(1);
        }
        this.type = type;
        this.socket = this.ctx.create_socket(type.getType());
        this.state.set(State.STARTED);
    }

    public SocketType getType() {
        return this.type;
    }

    public boolean connect(String endpoint) {
        return socket.connect(endpoint);
    }

    public boolean bind(String endpoint) {
        return socket.bind(endpoint);
    }

    public int send(byte[] buf, int pos, int lim, int flags) {
        final Msg msg = new Msg(buf, pos, lim);
        socket.send(msg, flags);
        mayRaise();
        return msg.size();
    }

    public byte[] receive(int flags) {
        final Msg msg = socket.recv(flags);
        if (msg != null) {
            return msg.data();
        }
        mayRaise();
        return null;
    }

    public ByteBuffer receiveByteBuffer(int flags) {
        final Msg msg = socket.recv(flags);
        if (msg != null) {
            return msg.buf();
        }
        mayRaise();
        return ByteBuffer.allocate(0);
    }

    private void mayRaise() {
        int errno = socket.errno();
        if (errno != 0 && errno != zmq.ZError.EAGAIN)
            throw new ZMQException(errno);
    }

    @Override
    public void close() {
        if (state.compareAndSet(State.STARTED, State.STOPPED)) {
            this.socket.close();
        }
    }
}
