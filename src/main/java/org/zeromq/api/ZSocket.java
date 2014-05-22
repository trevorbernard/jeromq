package org.zeromq.api;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.EnumSet;
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

    public boolean hasReceiveMore() {
        return socket.getsockopt(zmq.ZMQ.ZMQ_RCVMORE) == 1;
    }

    public int send(byte[] buf) {
        return this.send(buf, EnumSet.of(SocketFlags.NONE));
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

    public byte[] receive() {
        return this.receive(EnumSet.of(SocketFlags.NONE));
    }

    public byte[] receive(Set<SocketFlags> flags) {
        final Msg msg = socket.recv(calcFlags(flags));
        if (msg != null) {
            return msg.data();
        }
        mayRaise();
        return null;
    }

    public ByteBuffer receiveByteBuffer() {
        return receiveByteBuffer(EnumSet.of(SocketFlags.NONE));
    }

    public ByteBuffer receiveByteBuffer(Set<SocketFlags> flags) {
        final Msg msg = socket.recv(calcFlags(flags));
        if (msg != null) {
            return msg.buf();
        }
        mayRaise();
        return ByteBuffer.allocate(0);
    }

    public ZSocket setAffinity(long affinity) {
        socket.setsockopt(zmq.ZMQ.ZMQ_AFFINITY, affinity);
        return this;
    }

    public ZSocket setIdentity(byte[] identity) {
        socket.setsockopt(zmq.ZMQ.ZMQ_IDENTITY, identity);
        return this;
    }

    public ZSocket setRecoveryIVL(long recoveryIVL) {
        socket.setsockopt(zmq.ZMQ.ZMQ_RECOVERY_IVL, recoveryIVL);
        return this;
    }

    public ZSocket setRate(long rate) {
        socket.setsockopt(zmq.ZMQ.ZMQ_RATE, rate);
        return this;
    }

    public ZSocket setSendBuffer(int sendBuffer) {
        socket.setsockopt(zmq.ZMQ.ZMQ_SNDBUF, sendBuffer);
        return this;
    }

    public ZSocket setReceiveBuffer(int receiveBuffer) {
        socket.setsockopt(zmq.ZMQ.ZMQ_RCVBUF, receiveBuffer);
        return this;
    }

    public ZSocket setReceiveTimeout(int receiveTimeout) {
        socket.setsockopt(zmq.ZMQ.ZMQ_RCVTIMEO, receiveTimeout);
        return this;
    }

    public ZSocket setSendTimeout(int sendTimeout) {
        socket.setsockopt(zmq.ZMQ.ZMQ_SNDTIMEO, sendTimeout);
        return this;
    }

    public ZSocket setLinger(int linger) {
        socket.setsockopt(zmq.ZMQ.ZMQ_LINGER, linger);
        return this;
    }

    public ZSocket reconnectIVL(int reconnectIVL) {
        socket.setsockopt(zmq.ZMQ.ZMQ_RECONNECT_IVL, reconnectIVL);
        return this;
    }

    public ZSocket reconnectIVLMax(int reconnectIVLMax) {
        socket.setsockopt(zmq.ZMQ.ZMQ_RECONNECT_IVL_MAX, reconnectIVLMax);
        return this;
    }

    public ZSocket setBacklog(int value) {
        socket.setsockopt(zmq.ZMQ.ZMQ_BACKLOG, value);
        return this;
    }

    public ZSocket subscribe(byte[] topic) {
        socket.setsockopt(zmq.ZMQ.ZMQ_SUBSCRIBE, topic);
        return this;
    }

    public ZSocket unsubscribe(byte[] topic) {
        socket.setsockopt(zmq.ZMQ.ZMQ_UNSUBSCRIBE, topic);
        return this;
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
