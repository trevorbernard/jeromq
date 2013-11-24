package org.zeromq.api;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import zmq.Ctx;
import zmq.SocketBase;
import zmq.ZMQ;

public class Context implements Closeable {
    private final List<Socket> sockets = new ArrayList<Socket>();

    private final Ctx context;

    public Context() {
        this.context = ZMQ.zmq_ctx_new();
    }

    public Context(int ioThreads) {
        this();
        setIOThreads(ioThreads);
    }

    public int getIOThreads() {
        return context.get(ZMQ.ZMQ_IO_THREADS);
    }

    public void setIOThreads(int ioThreads) {
        context.set(ZMQ.ZMQ_IO_THREADS, ioThreads);
    }

    public int getMaxSockets() {
        return context.get(ZMQ.ZMQ_MAX_SOCKETS);
    }

    public void setMaxSockets(int maxSockets) {
        context.set(ZMQ.ZMQ_MAX_SOCKETS, maxSockets);
    }

    public synchronized Socket createSocket(SocketType type) {
        SocketBase base = context.create_socket(type.getValue());
        Socket s = new Socket(base, type);
        sockets.add(s);
        return s;
    }

    @Override
    public void close() {
        for (Socket s : sockets) {
            s.setLinger(0);
            s.close();
        }
        context.terminate();
    }
}