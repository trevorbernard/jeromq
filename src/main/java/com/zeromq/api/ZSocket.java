package com.zeromq.api;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.zeromq.ZMQException;

import zmq.Ctx;
import zmq.Msg;
import zmq.SocketBase;
import zmq.ZMQ;

public class ZSocket implements Closeable {
    // Lazy thread safe Context singleton
    private static class ContextHolder {
        private static final Ctx CONTEXT = ZMQ.zmq_ctx_new();
        static {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        CONTEXT.terminate();
                    } catch (Exception e) {
                    }
                }
            });
        }
    }

    private enum State {
        LATENT, STARTED, STOPPED;
    }

    private final AtomicReference<State> state = new AtomicReference<ZSocket.State>(State.LATENT);

    private final SocketType type;
    private final SocketBase socket;

    public ZSocket(SocketType type) {
        this.type = type;
        this.socket = getContext().create_socket(type.getType());
        this.state.set(State.STARTED);
    }

    private Ctx getContext() {
        return ContextHolder.CONTEXT;
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

    // This looks awkward but should be remedied if/when JeroMQ uses EnumSets
    private static int calcFlags(Set<SocketFlags> flags) {
        int f = 0;
        if (flags != null) {
            for (SocketFlags flag : flags) {
                f |= flag.getFlag();
            }
        }
        return f;
    }

    public int send(byte[] buf, Set<SocketFlags> flags) {
        final Msg msg = new Msg(buf);
        socket.send(msg, calcFlags(flags));
        mayRaise();
        return msg.size();
    }

    public int send(byte[] buf, int pos, int lim, Set<SocketFlags> flags) {
        final Msg msg = new Msg(buf, pos, lim);
        socket.send(msg, calcFlags(flags));
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

    public boolean isClosed() {
        return state.get() == State.STOPPED;
    }

    @Override
    public void close() {
        if (state.compareAndSet(State.STARTED, State.STOPPED)) {
            this.socket.close();
        }
    }
}
